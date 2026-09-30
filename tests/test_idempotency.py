"""Idempotent dispatch: distinct rejection reasons and exactly-once handling."""
from __future__ import annotations

import threading

import pytest

from idempotent_tasks.errors import (
    ContentConflictError,
    InvalidRequestError,
    KeyOccupiedError,
)
from idempotent_tasks.models import RetryPolicy, TaskState


def test_invalid_requests_are_distinct(make_dispatcher):
    d = make_dispatcher(lambda t: None)
    for bad_kwargs in (
        {"business_key": ""},
        {"business_key": "   "},
        {"business_key": "x" * 300},
        {"business_key": "k", "group_name": ""},
        {"business_key": "k", "payload": "x" * (64 * 1024 + 1)},
        {"business_key": 123},  # type: ignore[arg-type]
    ):
        with pytest.raises(InvalidRequestError) as exc:
            d.dispatch(**bad_kwargs)
        assert exc.value.reason.value == "invalid_request"


def test_duplicate_in_flight_is_replay_not_conflict(make_dispatcher, store):
    started = threading.Event()
    release = threading.Event()

    def handler(task):
        started.set()
        release.wait(timeout=2)

    d = make_dispatcher(handler)
    r1 = d.dispatch("k", group_name="g", payload="v1")
    assert r1.accepted and r1.state is TaskState.PENDING
    d.start()
    assert started.wait(1)

    # Same key+payload while processing: rejected softly, once only.
    r2 = d.dispatch("k", group_name="g", payload="v1")
    assert not r2.accepted
    assert r2.reason == "duplicate_in_flight"
    assert r2.task_id == r1.task_id

    release.set()
    assert d.wait_until_drained(2)
    assert store.get(r1.task_id).state is TaskState.SUCCEEDED


def test_key_occupied_vs_content_conflict_are_distinguished(
        make_dispatcher, store):
    d = make_dispatcher(lambda t: None)
    d.dispatch("k", payload="v1")
    # Still pending (no workers started): different content => occupied.
    with pytest.raises(KeyOccupiedError) as occupied:
        d.dispatch("k", payload="v2")
    assert occupied.value.reason.value == "key_occupied"
    assert occupied.value.existing_task_id >= 1

    d.start()
    assert d.wait_until_drained(2)
    # After success: same payload is a replay...
    replay = d.dispatch("k", payload="v1")
    assert replay.reason == "replay_succeeded"
    # ...different payload is now a content conflict (422-class), not occupied.
    with pytest.raises(ContentConflictError) as conflict:
        d.dispatch("k", payload="v2")
    assert conflict.value.reason.value == "content_conflict"
    assert conflict.value.existing_task_id >= 1


def test_succeeded_task_is_never_executed_again(make_dispatcher, store):
    calls: list[str] = []

    def handler(task):
        calls.append(task.business_key)

    d = make_dispatcher(handler)
    r = d.dispatch("once", payload="p")
    d.start()
    assert d.wait_until_drained(2)
    d.shutdown()
    assert calls == ["once"]

    # Re-deliver identical request: no new row, no new execution.
    replay = d.dispatch("once", payload="p")
    assert replay.reason == "replay_succeeded"
    assert replay.task_id == r.task_id
    d.start()
    assert d.wait_until_drained(2)
    assert calls == ["once"]
    assert store.counters().total == 1


def test_dead_replay_does_not_rerun(make_dispatcher, store):
    calls = 0

    def handler(task):
        nonlocal calls
        calls += 1
        raise RuntimeError("permanent failure")

    d = make_dispatcher(
        handler, policy=RetryPolicy(max_attempts=1, base_delay=0.01))
    d.dispatch("doomed", payload="p")
    d.start()
    assert d.wait_until_drained(2)
    assert calls == 1
    d.shutdown()

    replay = d.dispatch("doomed", payload="p")
    assert replay.reason == "replay_dead"
    d.start()
    assert d.wait_until_drained(2)
    assert calls == 1
    assert store.counters().dead == 1
