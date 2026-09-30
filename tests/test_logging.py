"""日志必须打出任务键、尝试次数与判定理由。"""
from __future__ import annotations

import logging

from taskdispatch.service import TaskService
from taskdispatch.worker import Worker

from .conftest import make_worker


def _records(caplog, name: str) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == name]


def test_dispatch_logs_carry_key_and_reason(caplog, service: TaskService) -> None:
    caplog.set_level(logging.INFO, logger="taskdispatch.service")
    service.dispatch("g", "k", {"v": 1})
    service.dispatch("g", "k2", {"v": 2})

    msgs = [r.getMessage() for r in _records(caplog, "taskdispatch.service")]
    accepted = [m for m in msgs if "reason=new" in m]
    assert len(accepted) == 2
    assert all("key=" in m and "group=" in m and "task_id=" in m for m in accepted)


def test_retry_and_dead_logs_carry_attempts_and_reason(caplog, store) -> None:
    service = TaskService(store, max_attempts=2)
    service.dispatch("g", "k", {"v": 1})

    def handler(record):
        raise RuntimeError("boom")

    caplog.set_level(logging.INFO, logger="taskdispatch.worker")
    w = make_worker(store, "g", handler)
    w.run_until_idle(timeout=5)

    msgs = [r.getMessage() for r in _records(caplog, "taskdispatch.worker")]
    retry = [m for m in msgs if "reason=handler_failed" in m]
    dead = [m for m in msgs if "reason=retries_exhausted" in m]
    assert len(retry) == 1
    assert len(dead) == 1
    for m in retry + dead:
        assert "key=k" in m
        assert "attempts=" in m
    # 重试日志应带上当前尝试 1/2 与退避字段
    assert "attempts=1/2" in retry[0] and "retry_in=" in retry[0]
    assert "attempts=2/2" in dead[0]


def test_timeout_log_reason(caplog, store) -> None:
    service = TaskService(store, max_attempts=2)
    service.dispatch("g", "slow", {})

    import time

    def slow(record):
        # 远长于租约（保证第 1 次必然超时），但短于测试总时长，
        # 旧线程结束后第 2 次尝试才能领取并最终进死信
        time.sleep(0.8)
        return "late"

    caplog.set_level(logging.WARNING, logger="taskdispatch.worker")
    w = make_worker(store, "g", slow, lease_seconds=0.05, backoff=lambda a: 0.0)
    w.start()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        rec = store.get_by_key("g", "slow")
        if rec.state.value == "DEAD":
            break
        time.sleep(0.02)
    w.stop(wait=True)
    assert store.get_by_key("g", "slow").state.value == "DEAD"

    msgs = [r.getMessage() for r in _records(caplog, "taskdispatch.worker")]
    timeouts = [m for m in msgs if "task timeout" in m]
    assert timeouts, msgs
    assert "key=slow" in timeouts[0] and "attempts=1/2" in timeouts[0]
