from __future__ import annotations

import threading
from collections.abc import Callable, Iterable

import pytest

from taskdispatch.service import TaskService
from taskdispatch.store import TaskStore
from taskdispatch.worker import Worker


@pytest.fixture
def store() -> Iterable[TaskStore]:
    s = TaskStore(":memory:")
    yield s
    s.close()


@pytest.fixture
def service(store) -> TaskService:
    return TaskService(store, max_attempts=3)


class Barrier:
    """测试用栅栏：放行 release 个正在等待的 handler 调用。"""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._permits = 0

    def release(self, n: int = 1) -> None:
        with self._cond:
            self._permits += n
            self._cond.notify_all()

    def wait(self) -> None:
        with self._cond:
            while self._permits <= 0:
                self._cond.wait(timeout=5)
                if self._permits <= 0:
                    raise TimeoutError("barrier not released")
            self._permits -= 1


def make_worker(
    store: TaskStore,
    group: str,
    handler: Callable,
    **kwargs,
) -> Worker:
    kwargs.setdefault("concurrency", 8)
    kwargs.setdefault("lease_seconds", 30.0)
    kwargs.setdefault("poll_interval", 0.005)
    kwargs.setdefault("backoff", lambda attempts: 0.0)
    return Worker(store, group, handler, **kwargs)
