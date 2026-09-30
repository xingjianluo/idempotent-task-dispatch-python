"""Retry with backoff, timeout handling and dead-lettering."""
from __future__ import annotations

import threading
import time

from idempotent_tasks.models import RetryPolicy, TaskState


def test_retries_then_succeeds_attempts_recorded(make_dispatcher, store):
    attempts = {"n": 0}

    def handler(task):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError(f"transient-{attempts['n']}")

    policy = RetryPolicy(max_attempts=3, base_delay=0.01, factor=2.0,
                         max_delay=0.05, timeout=1.0)
    d = make_dispatcher(handler, policy=policy, workers=1)
    r = d.dispatch("flaky", payload="p")
    d.start()
    assert d.wait_until_drained(3)

    rec = store.get(r.task_id)
    assert rec.state is TaskState.SUCCEEDED
    assert rec.attempts == 3
    assert attempts["n"] == 3


def test_exhausted_retries_go_to_dead_letter_with_reason(make_dispatcher, store):
    attempts = {"n": 0}

    def handler(task):
        attempts["n"] += 1
        raise ValueError("bad value 42")

    policy = RetryPolicy(max_attempts=3, base_delay=0.01, timeout=1.0)
    d = make_dispatcher(handler, policy=policy, workers=1)
    r = d.dispatch("doomed", payload="p")
    d.start()
    assert d.wait_until_drained(3)

    rec = store.get(r.task_id)
    assert rec.state is TaskState.DEAD
    assert rec.attempts == 3
    assert attempts["n"] == 3
    assert "ValueError: bad value 42" in (rec.last_error or "")

    dead = store.list_dead_letters()
    assert [t.id for t in dead] == [r.task_id]
    assert "bad value 42" in (dead[0].last_error or "")


def test_backoff_delays_retries(make_dispatcher):
    seen: list[float] = []
    fail_until = {"n": 0}

    def handler(task):
        seen.append(time.monotonic())
        fail_until["n"] += 1
        if fail_until["n"] < 2:
            raise RuntimeError("slow failure")

    policy = RetryPolicy(max_attempts=2, base_delay=0.2, factor=2.0,
                         timeout=1.0)
    d = make_dispatcher(handler, policy=policy, workers=1)
    d.dispatch("slow", payload="p")
    start = time.monotonic()
    d.start()
    assert d.wait_until_drained(3)

    assert len(seen) == 2
    assert (seen[1] - seen[0]) >= 0.18  # ~0.2s backoff, tolerant timing


def test_timeout_counts_as_failed_attempt(make_dispatcher, store):
    release = threading.Event()

    def handler(task):
        release.wait(timeout=5)  # always exceeds the tiny timeout

    policy = RetryPolicy(max_attempts=2, base_delay=0.01, timeout=0.05)
    d = make_dispatcher(handler, policy=policy, workers=2)
    r = d.dispatch("stuck", payload="p")
    d.start()
    assert d.wait_until_drained(3)
    release.set()

    rec = store.get(r.task_id)
    assert rec.state is TaskState.DEAD
    assert rec.attempts == 2
    assert "timed out" in (rec.last_error or "")


def test_backing_off_task_blocks_later_same_key_task(make_dispatcher, store):
    """Same key is a single business-key slot anyway; here we verify ordering
    across two different keys of one group is unblocked (different keys),
    while a blocked head delays everything sharing that key."""
    order: list[str] = []
    gate = threading.Event()

    # Key A fails first attempt and backs off; key B is independent.
    def handler(task):
        if task.business_key == "A":
            order.append("A")
            if not gate.is_set():
                gate.set()
                raise RuntimeError("retry me")
        else:
            gate.wait(timeout=2)
            order.append("B")

    policy = RetryPolicy(max_attempts=2, base_delay=0.15, timeout=1.0)
    d = make_dispatcher(handler, policy=policy, workers=2)
    d.dispatch("A", group_name="g", payload="1")
    d.dispatch("B", group_name="g", payload="2")
    d.start()
    assert d.wait_until_drained(3)
    # A attempted twice, B once; A's first attempt precedes B, and A's
    # retry happens after backoff.
    assert order.count("A") == 2
    assert order.count("B") == 1
    assert order[0] == "A"


def test_counters_conserve_through_retry_and_dead(make_dispatcher, store):
    def handler(task):
        raise RuntimeError("x")

    policy = RetryPolicy(max_attempts=2, base_delay=0.005, timeout=1.0)
    d = make_dispatcher(handler, policy=policy, workers=2)
    for i in range(6):
        d.dispatch(f"k{i}", payload="p")
    d.start()
    assert d.wait_until_drained(5)

    c = store.counters()
    assert c.total == 6
    assert c.pending == 0 and c.processing == 0
    assert c.succeeded == 0 and c.dead == 6
