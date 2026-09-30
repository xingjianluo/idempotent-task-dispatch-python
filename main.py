"""Application entry point: wire store, handler, dispatcher and FastAPI app.

Override the database path with TASK_DB_PATH (defaults to tasks.db next to
this file). The example handler is intentionally simple; swap in real work.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import uvicorn

from idempotent_tasks.api import create_app
from idempotent_tasks.dispatcher import Dispatcher
from idempotent_tasks.logging_setup import configure_logging
from idempotent_tasks.models import RetryPolicy, TaskRecord
from idempotent_tasks.store import TaskStore

DEFAULT_DB = str(Path(__file__).with_name("tasks.db"))


def example_handler(task: TaskRecord) -> None:
    """Example business handler. Raise to simulate a failing task."""
    logging.getLogger("idempotent_tasks").info(
        "handling payload length=%d", len(task.payload),
        extra={"task_key": task.business_key, "attempt": task.attempts + 1,
               "decision": "handle"},
    )


def build_app() -> "FastAPI":  # pragma: no cover - thin wiring
    from fastapi import FastAPI

    configure_logging(logging.INFO)
    store = TaskStore(os.environ.get("TASK_DB_PATH", DEFAULT_DB))
    store.init_schema()
    dispatcher = Dispatcher(
        store,
        example_handler,
        retry_policy=RetryPolicy(
            max_attempts=3, base_delay=1.0, factor=2.0,
            max_delay=30.0, timeout=5.0,
        ),
        workers=2,
    )
    return create_app(store, dispatcher)


app = build_app()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
