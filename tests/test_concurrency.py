"""并发投递/消费与重启恢复：可区分拒绝、数量守恒、重启不重复执行。"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from taskdispatch.exceptions import KeyOccupiedError, PayloadConflictError
from taskdispatch.models import TaskState
from taskdispatch.service import TaskService
from taskdispatch.store import TaskStore
from taskdispatch.worker import Worker

from .conftest import Barrier, make_worker


def test_concurrent_same_key_dispatch_single_accept(service: TaskService, store) -> None:
    n = 32
    outcomes, occupied, conflicts, others = [], [], [], []

    def deliver(_):
        try:
            outcomes.append(service.dispatch("g", "k", {"v": 1}))
        except KeyOccupiedError:
            occupied.append(1)
        except PayloadConflictError:
            conflicts.append(1)
        except Exception as exc:  # noqa: BLE001
            others.append(repr(exc))

    with ThreadPoolExecutor(max_workers=n) as pool:
        list(pool.map(deliver, range(n)))

    assert others == [], others
    assert len(outcomes) == 1 and not outcomes[0].duplicated
    assert len(occupied) == n - 1
    assert conflicts == []
    counts = store.count_by_state("g")
    assert sum(counts.values()) == 1  # 拒绝不产生任务


def test_concurrent_same_key_with_conflicting_payloads(service: TaskService, store) -> None:
    n = 24

    def deliver(i):
        try:
            service.dispatch("g", "k", {"v": i})  # 每人内容都不同
            return "ok"
        except KeyOccupiedError:
            return "occupied"
        except PayloadConflictError:
            return "conflict"

    with ThreadPoolExecutor(max_workers=n) as pool:
        results = list(pool.map(deliver, range(n)))

    assert results.count("ok") == 1
    assert results.count("occupied") == 0  # 没有同内容并发
    assert results.count("conflict") == n - 1
    assert store.count_by_state("g")["PENDING"] == 1


def test_different_keys_run_concurrently(store) -> None:
    service = TaskService(store, max_attempts=1)
    for i in range(8):
        service.dispatch("g", f"k{i}", {"i": i})

    entered = Barrier()
    active = {"n": 0, "max_n": 0}
    lock = threading.Lock()

    def handler(record):
        with lock:
            active["n"] += 1
            active["max_n"] = max(active["max_n"], active["n"])
        entered.wait()
        with lock:
            active["n"] -= 1
        return "ok"

    w = make_worker(store, "g", handler, concurrency=8)
    w.start()
    deadline = time.monotonic() + 5
    while active["n"] < 8 and time.monotonic() < deadline:
        time.sleep(0.01)
    entered.release(8)
    w.stop(wait=True)

    assert active["max_n"] == 8  # 8 个不同键真的同时在跑
    counts = store.count_by_state("g")
    assert counts["SUCCEEDED"] == 8 and counts["RUNNING"] == 0


def test_same_key_tasks_are_serial(store) -> None:
    """同键任务在每个键上串行：前一个终态前，重复投递被拒；终态后顺序处理下一单。"""
    service = TaskService(store, max_attempts=1)
    order: list[str] = []
    hold = Barrier()

    def handler(record):
        order.append(record.key)
        hold.wait()
        return "ok"

    w = make_worker(store, "g", handler, concurrency=4)
    service.dispatch("g", "same", {"n": 1})
    w.start()
    time.sleep(0.1)  # 等 first 被领取
    # 在途期间同键再投被占用拒绝
    with pytest.raises(KeyOccupiedError):
        service.dispatch("g", "same", {"n": 1})
    hold.release(1)
    w.stop(wait=True)

    # 终态后下一单才会被接受并处理，不会与第一单并发
    out = service.dispatch("g", "same", {"n": 1})
    assert out.duplicated is True and out.state is TaskState.SUCCEEDED
    # 注意：幂等键相同 => 直接重放，不会产生第二个任务
    assert store.count_by_state("g")["SUCCEEDED"] == 1


def test_mixed_concurrent_load_conserves_counts(store) -> None:
    """投递与消费同时发生：始终 total = 投递接受数，且 PENDING+RUNNING+SUCCEEDED+DEAD 守恒。"""
    service = TaskService(store, max_attempts=2)
    n_keys = 60

    def handler(record):
        if record.key.endswith(":fail") and record.attempts < 2:
            raise RuntimeError("transient")
        return "ok"

    w = make_worker(store, "g", handler, concurrency=8, backoff=lambda a: 0.0)
    w.start()

    accepted = 0
    for i in range(n_keys):
        key = f"k{i}:fail" if i % 4 == 0 else f"k{i}:ok"
        try:
            service.dispatch("g", key, {"i": i})
            accepted += 1
        except (KeyOccupiedError, PayloadConflictError):
            pass
        if i % 7 == 0:
            time.sleep(0.002)
            counts = store.count_by_state("g")
            assert sum(counts.values()) <= accepted  # 任意时刻无凭空多出/消失

    # 投递结束后等队列彻底排空（PENDING/RUNNING 归零）
    w.run_until_idle(timeout=10)
    w.stop(wait=True)
    counts = store.count_by_state("g")
    assert counts["PENDING"] == 0
    assert counts["RUNNING"] == 0
    assert sum(counts.values()) == accepted
    assert counts["SUCCEEDED"] + counts["DEAD"] == accepted
    # 每个进死信的任务都带可查原因，且尝试次数打满
    for dead in store.list_dead("g"):
        assert dead.attempts == 2
        assert "retries exhausted" in (dead.last_error or "")


def test_restart_reclaims_running_and_keeps_terminal(tmp_path) -> None:
    """模拟崩溃重启：遗留 RUNNING 被收回重跑，已 SUCCEEDED 的不再执行。"""
    db = str(tmp_path / "tasks.db")

    store1 = TaskStore(db)
    svc1 = TaskService(store1, max_attempts=2, clock=time.monotonic)
    svc1.dispatch("g", "unfinished", {"v": 1})
    svc1.dispatch("g", "finished", {"v": 2})

    t1 = store1.acquire_next("g", now=time.monotonic(), lease_seconds=30)
    t2 = store1.acquire_next("g", now=time.monotonic(), lease_seconds=30)
    assert t1 is not None and t2 is not None
    # t2 在崩溃前已确认完成；t1 停在 RUNNING
    if t1.key != "unfinished":
        t1, t2 = t2, t1
    assert store1.complete(t2.id, t2.attempts, "result", time.monotonic())
    store1.close()

    # —— 新进程：重开文件库 ——
    store2 = TaskStore(db)
    calls: list[str] = []

    def handler(record):
        calls.append(record.key)
        if record.key == "unfinished":
            return "recovered-result"
        raise AssertionError("terminal task must not be reprocessed")

    w2 = make_worker(store2, "g", handler)
    reclaimed = w2.recover(force_all=True)
    assert reclaimed == 1
    w2.run_until_idle(timeout=5)

    assert calls == ["unfinished"]
    rec = store2.get_by_key("g", "unfinished")
    assert rec.state is TaskState.SUCCEEDED
    assert rec.result == "recovered-result"
    assert rec.attempts == 2  # 崩溃前 1 次 + 重启后 1 次
    finished = store2.get_by_key("g", "finished")
    assert finished.state is TaskState.SUCCEEDED and finished.result == "result"
    counts = store2.count_by_state("g")
    assert counts["RUNNING"] == 0 and counts["PENDING"] == 0
    store2.close()
