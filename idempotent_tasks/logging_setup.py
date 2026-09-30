"""Logging helpers.

Every task lifecycle log line is emitted with extra fields ``task_key``,
``attempt`` and ``decision`` (the judgement reason), e.g.::

    2026-09-30 12:00:00 INFO ... task_key=order-123 attempt=2 decision=retry ...
"""
from __future__ import annotations

import logging

_DEFAULT_FORMAT = (
    "%(asctime)s %(levelname)s %(name)s %(message)s "
    "task_key=%(task_key)s attempt=%(attempt)s decision=%(decision)s"
)


class _TaskDefaults(logging.Filter):
    """Make task_key/attempt/decision always present on records."""

    def filter(self, record: logging.LogRecord) -> bool:
        for field, default in (
            ("task_key", "-"),
            ("attempt", "-"),
            ("decision", "-"),
        ):
            if not hasattr(record, field):
                setattr(record, field, default)
        return True


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger("idempotent_tasks")
    logger.setLevel(level)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT))
        handler.addFilter(_TaskDefaults())
        logger.addHandler(handler)
    logger.propagate = False
    return logger
