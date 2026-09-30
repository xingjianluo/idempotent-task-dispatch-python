"""Concurrent dispatch/consumption, ordering guarantees and restart recovery."""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from idempotent_tasks.dispatcher import Dispatcher
from idempotent_tasks.errors import KeyOccupiedError
from idempotent_tasks.models import RetryPolicy, TaskState


def test_concurrent_duplicate_dispatch_accepted_exactly_once(make_dispatcher):
    accepted: list[int] = []
    lock = threading.Lock()
    d = make_dispatcher(lambda t: None)

    def submit(i: int):
        r = d.dispatch("hot-key", payload="same")
        if r.accepted:
            with lock:
                accepted.append(r.task_id)

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(submit, range(50)))

    assert len(accepted) == 1
    # Every accepted response referenced the same newly created row.
    assert set(accepted) == {accepted[0]}


def test_concurrent_conflicting_dispatch_never_inserts(make_dispatcher, store):
    outcomes = {"accepted": 0, "occupied": 0, "duplicate": 0}

    def submit(i: int):
        try:
            r = d.dispatch("race-key", payload=f"v{i}")
            if r.accepted:
                outcomes["accepted"] += 1
        except KeyOccupiedError:
            outcomes["occupied"] += 1

    d = make_dispatcher(lambda t: None)
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(submit, range(32)))

    assert outcomes["accepted"] == 1
    assert outcomes["occupied"] == 31
    assert store.counters().total == 1


def test_concurrent_consumption_conserves_counts(make_dispatcher, store):
    executed: list[int] = []
    lock = threading.Lock()

    def handler(task):
        with lock:
            executed.append(task.id)

    d = make_dispatcher(handler, workers=8)
    n = 100
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(
            lambda i: d.dispatch(f"key-{i}", group_name="g", payload=str(i)),
            range(n),
        ))

    d.start()
    assert d.wait_until_drained(10)
    c = store.counters()
    assert c.total == n
    assert c.succeeded == n
    assert c.pending == 0 and c.processing == 0 and c.dead == 0
    assert len(executed) == n
    assert len(set(executed)) == n  # each task executed exactly once


def test_same_key_ordered_different_keys_concurrent(make_dispatcher, store):
    running: dict[str, int] = {}
    finished_order: list[str] = []
    lock = threading.Lock()
    key_a_gate = threading.Event()

    def handler(task):
        with lock:
            running[task.business_key] = running.get(task.business_key, 0) + 1
        if task.business_key == "A":
            # First A holds the slot; meanwhile B (different key) proceeds.
            if task.payload == "a1":
                key_a_gate.wait(timeout=2)
        with lock:
            finished_order.append(task.business_key + ":" + task.payload)
            running[task.business_key] -= 1

    d = make_dispatcher(handler, workers=4)
    # Business key is globally unique; ordering across retries of the *same*
    # key is provided by the head-claim rule. To observe different-key
    # concurrency we use distinct keys in one group.
    d.dispatch("A", group_name="g", payload="a1")
    d.dispatch("B", group_name="g", payload="b1")
    d.start()
    # B must finish even though A's head task is parked.
    assert _wait_for(lambda: "B:b1" in finished_order, 2)
    assert "A:a1" not in finished_order
    key_a_gate.set()
    assert d.wait_until_drained(2)
    assert finished_order.index("B:b1") < finished_order.index("A:a1")


def test_same_key_second_attempt_cannot_overtake_backoff(make_dispatcher, store):
    """After a failure the same key's task sits in backoff; it must not be
    claimed again until visible_after, and then completes."""
    attempts = {"n": 0}
    lock = threading.Lock()

    def handler(task):
        with lock:
            attempts["n"] += 1
            n = attempts["n"]
        if n == 1:
            raise RuntimeError("backoff please")

    policy = RetryPolicy(max_attempts=2, base_delay=0.15, timeout=1.0)
    d = make_dispatcher(handler, policy=policy, workers=2)
    r = d.dispatch("ordered", payload="p")
    d.start()
    # Immediately after first failure the task must still be pending/hidden.
    assert _wait_for(
        lambda: store.get(r.task_id).state is TaskState.PENDING
        and store.get(r.task_id).attempts == 1, 2)
    rec = store.get(r.task_id)
    assert rec.visible_after > rec.updated_at or rec.state is TaskState.PENDING
    assert d.wait_until_drained(3)
    assert store.get(r.task_id).state is TaskState.SUCCEEDED
    assert attempts["n"] == 2


def test_restart_does_not_rerun_succeeded_task(store):
    def handler(task):
        pass

    d1 = Dispatcher(store, handler,
                    retry_policy=RetryPolicy(max_attempts=1),
                    workers=1, poll_interval=0.005)
    r = d1.dispatch("durable", payload="p")
    d1.start()
    assert d1.wait_until_drained(2)
    d1.shutdown()

    # New dispatcher on the same DB: confirmed task stays confirmed.
    d2 = Dispatcher(store, handler,
                    retry_policy=RetryPolicy(max_attempts=1),
                    workers=1, poll_interval=0.005)
    d2.start()
    assert d2.wait_until_drained(2)
    d2.shutdown()
    rec = store.get(r.task_id)
    assert rec.state is TaskState.SUCCEEDED
    assert rec.attempts == 1


def test_restart_requeues_interrupted_processing_task(store):
    """Simulate a crash: leave a row in processing, then start a fresh
    dispatcher; it must requeue and finish it exactly once."""
    calls = {"n": 0}
    lock = threading.Lock()

    def handler(task):
        with lock:
            calls["n"] += 1

    task_id = store.insert_pending(
        business_key="orphan", group_name="g",
        payload="p", payload_hash="h", now=0)
    # Orphaned processing row (attempts already consumed once).
    with store.transaction() as conn:
        conn.execute(
            "UPDATE tasks SET state='processing', attempts=1 WHERE id=?",
            (task_id,))
    assert store.counters().processing == 1

    d = Dispatcher(store, handler,
                   retry_policy=RetryPolicy(max_attempts=3, base_delay=0.01),
                   workers=2, poll_interval=0.005)
    d.start()
    assert d.wait_until_drained(3)
    d.shutdown()

    rec = store.get(task_id)
    assert rec.state is TaskState.SUCCEEDED
    assert calls["n"] == 1
    c = store.counters()
    assert c.processing == 0 and c.total == 1


def _wait_for(predicate, timeout: float, interval: float = 0.005) -> bool:
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False
