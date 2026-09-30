"""Logs must carry business key, attempt number and the decision reason."""
from __future__ import annotations

import logging
import threading

from idempotent_tasks.models import RetryPolicy


def test_retry_and_dead_logs_carry_key_attempt_and_decision(
        make_dispatcher, caplog):
    caplog.set_level(logging.WARNING, logger="idempotent_tasks.test")

    def handler(task):
        raise RuntimeError("log me")

    policy = RetryPolicy(max_attempts=2, base_delay=0.01, timeout=1.0)
    d = make_dispatcher(handler, policy=policy, workers=1)
    d.dispatch("logged-key", payload="p")
    d.start()
    assert d.wait_until_drained(3)

    def field(record, name):
        return getattr(record, name, None)

    retries = [r for r in caplog.records if field(r, "decision") == "retry"]
    dead = [r for r in caplog.records if field(r, "decision") == "dead"]
    assert len(retries) == 1 and len(dead) == 1
    rec = retries[0]
    assert field(rec, "task_key") == "logged-key"
    assert field(rec, "attempt") == 1
    assert "log me" in rec.getMessage()

    dead_rec = dead[0]
    assert field(dead_rec, "task_key") == "logged-key"
    assert field(dead_rec, "attempt") == 2


def test_dispatch_and_claim_logs_are_structured(make_dispatcher, caplog):
    caplog.set_level(logging.INFO, logger="idempotent_tasks.test")
    gate = threading.Event()

    def handler(task):
        gate.wait(timeout=1)

    d = make_dispatcher(handler)
    d.dispatch("struct-key", payload="p")
    d.start()
    gate.set()
    assert d.wait_until_drained(2)

    decisions = {
        getattr(r, "decision", None) for r in caplog.records
    }
    assert "accepted" in decisions
    assert "claimed" in decisions
    assert "succeeded" in decisions
    for r in caplog.records:
        if getattr(r, "decision", None) in ("claimed", "succeeded"):
            assert getattr(r, "task_key", None) == "struct-key"
            assert getattr(r, "attempt", None) is not None

