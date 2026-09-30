"""幂等投递：非法请求 / 占用 / 冲突 / 幂等重放必须可区分。"""
from __future__ import annotations

import pytest

from taskdispatch.exceptions import (
    InvalidRequestError,
    KeyOccupiedError,
    PayloadConflictError,
)
from taskdispatch.models import TaskState


def test_invalid_requests_are_rejected(service: "TaskService") -> None:
    with pytest.raises(InvalidRequestError):
        service.dispatch("", "k", {"v": 1})
    with pytest.raises(InvalidRequestError):
        service.dispatch("g", "  ", {"v": 1})
    with pytest.raises(InvalidRequestError):
        service.dispatch("g", "k", None)
    with pytest.raises(InvalidRequestError):
        service.dispatch("g", "k", {"v": 1}, max_attempts=0)
    with pytest.raises(InvalidRequestError):
        service.dispatch("g", "k", object())  # 不可 JSON 序列化


def test_three_distinct_rejections(service: "TaskService", store) -> None:
    out = service.dispatch("g", "k", {"v": 1})
    assert not out.duplicated and out.state is TaskState.PENDING

    # 在途 + 同内容 => 键被占用
    with pytest.raises(KeyOccupiedError) as occ:
        service.dispatch("g", "k", {"v": 1})
    assert occ.value.key == "k" and occ.value.group == "g"

    # 在途 + 不同内容 => 内容冲突
    with pytest.raises(PayloadConflictError):
        service.dispatch("g", "k", {"v": 2})

    assert store.count_by_state("g")["PENDING"] == 1  # 拒绝不产生新任务


def test_payload_key_order_does_not_conflict(service: "TaskService") -> None:
    service.dispatch("g", "k", {"a": 1, "b": 2})
    with pytest.raises(KeyOccupiedError):
        # 字段顺序不同但内容相同 => 仍是重复投递（占用），不是冲突
        service.dispatch("g", "k", {"b": 2, "a": 1})


def test_idempotent_replay_after_terminal(service: "TaskService", store) -> None:
    first = service.dispatch("g", "k", {"v": 1})

    record = store.acquire_next("g", now=10**12, lease_seconds=30)
    assert record is not None
    assert store.complete(record.id, record.attempts, "result", now=10**12)

    replay = service.dispatch("g", "k", {"v": 1})
    assert replay.duplicated is True
    assert replay.task_id == first.task_id
    assert replay.state is TaskState.SUCCEEDED
    assert store.count_by_state("g")["SUCCEEDED"] == 1

    # 终态后改内容仍然是冲突
    with pytest.raises(PayloadConflictError):
        service.dispatch("g", "k", {"v": 999})


def test_same_key_in_different_groups_is_independent(service: "TaskService") -> None:
    a = service.dispatch("g1", "k", {"v": 1})
    b = service.dispatch("g2", "k", {"v": 1})
    assert a.task_id != b.task_id
