"""失败重试、退避、重试耗尽进死信、失败原因可查。"""
from __future__ import annotations

from taskdispatch.models import TaskState
from taskdispatch.service import TaskService
from taskdispatch.worker import Worker

from .conftest import make_worker


def test_retries_until_success(service: TaskService, store) -> None:
    service.dispatch("g", "k", {"v": 1}, max_attempts=3)
    seen: list[int] = []

    def handler(record):
        seen.append(record.attempts)
        if record.attempts < 2:
            raise ValueError(f"fail-{record.attempts}")
        return "ok"

    w = make_worker(store, "g", handler)
    w.run_until_idle(timeout=5)
    assert seen == [1, 2]
    rec = store.get_by_key("g", "k")
    assert rec.state is TaskState.SUCCEEDED
    assert rec.result == "ok"
    assert rec.attempts == 2


def test_exhausted_retries_go_dead_with_reason(service: TaskService, store) -> None:
    service.dispatch("g", "k", {"v": 1}, max_attempts=3)
    seen: list[int] = []

    def handler(record):
        seen.append(record.attempts)
        raise RuntimeError(f"boom-{record.attempts}")

    w = make_worker(store, "g", handler)
    w.run_until_idle(timeout=5)

    assert seen == [1, 2, 3]
    rec = store.get_by_key("g", "k")
    assert rec.state is TaskState.DEAD
    assert rec.attempts == 3
    # 失败原因可查：耗尽说明 + 最后一次异常类型与消息
    assert rec.last_error is not None
    assert "retries exhausted after 3 attempts" in rec.last_error
    assert "RuntimeError" in rec.last_error
    assert "boom-3" in rec.last_error

    dead = store.list_dead("g")
    assert [d.id for d in dead] == [rec.id]
    assert dead[0].last_error == rec.last_error


def test_backoff_delays_retry(store) -> None:
    now = [1000.0]
    service = TaskService(store, max_attempts=2, clock=lambda: now[0])

    def handler(record):
        raise RuntimeError("nope")

    w = Worker(
        store,
        "g",
        handler,
        lease_seconds=1000.0,
        backoff=lambda a: 0.25,
        clock=lambda: now[0],
        sleep=lambda s: now.__setitem__(0, now[0] + s),
    )
    service.dispatch("g", "k", {"v": 1})

    # 第一轮 pump：领取并执行第一次尝试，失败后进入退避
    w._pump()
    while store.get_by_key("g", "k").state is TaskState.RUNNING:
        pass  # handler 在池线程里，等它完成状态流转
    rec = store.get_by_key("g", "k")
    assert rec.state is TaskState.PENDING
    assert rec.attempts == 1
    assert abs(rec.available_at - (now[0] + 0.25)) < 1e-9
    # 到期前不可领取
    assert store.acquire_next("g", now=now[0], lease_seconds=5) is None

    # 越过退避点：第二次尝试执行，重试耗尽进死信
    now[0] += 0.25
    w.run_until_idle(timeout=10)
    rec = store.get_by_key("g", "k")
    assert rec.state is TaskState.DEAD
    assert rec.attempts == 2


def test_dead_task_is_not_retried_after_restart(store) -> None:
    service = TaskService(store, max_attempts=1)
    service.dispatch("g", "k", {"v": 1})
    calls: list[int] = []
    w1 = make_worker(store, "g", lambda r: (_ for _ in ()).throw(RuntimeError("x")))
    w1.run_until_idle(timeout=5)
    assert store.get_by_key("g", "k").state is TaskState.DEAD

    # 模拟重启：新 worker 的 start/recover 不得动死信任务
    w2 = make_worker(store, "g", lambda r: calls.append(1) or "ok")
    reclaimed = w2.recover(force_all=True)
    w2.run_until_idle(timeout=5)
    assert reclaimed == 0
    assert calls == []
    assert store.get_by_key("g", "k").state is TaskState.DEAD
