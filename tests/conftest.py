"""Shared pytest fixtures: isolated temp DB per test and a dispatcher factory."""
from __future__ import annotations

import logging
from pathlib import Path

import pytest

from idempotent_tasks.dispatcher import Dispatcher, Handler
from idempotent_tasks.models import RetryPolicy
from idempotent_tasks.store import TaskStore

FastRetry = RetryPolicy(max_attempts=3, base_delay=0.01, factor=2.0,
                        max_delay=0.05, timeout=1.0)


@pytest.fixture
def store(tmp_path: Path) -> TaskStore:
    s = TaskStore(str(tmp_path / "tasks.db"))
    s.init_schema()
    yield s
    s.close()


@pytest.fixture
def make_dispatcher(store: TaskStore):
    created: list[Dispatcher] = []

    def _make(handler: Handler, *, policy: RetryPolicy | None = None,
              workers: int = 2) -> Dispatcher:
        d = Dispatcher(
            store, handler,
            retry_policy=policy or FastRetry,
            workers=workers,
            poll_interval=0.005,
            logger=logging.getLogger("idempotent_tasks.test"),
        )
        created.append(d)
        return d

    yield _make

    for d in created:
        d.shutdown()
