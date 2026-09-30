"""领域模型：任务状态、任务记录与投递结果。"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class TaskState(str, Enum):
    PENDING = "PENDING"       # 等待中
    RUNNING = "RUNNING"       # 处理中（被租约占用）
    SUCCEEDED = "SUCCEEDED"   # 已完成（确认过，重启不再执行）
    DEAD = "DEAD"             # 死信（重试耗尽）


@dataclass(frozen=True)
class TaskRecord:
    id: int
    group: str
    key: str
    payload: str
    payload_hash: str
    state: TaskState
    attempts: int
    max_attempts: int
    available_at: float
    leased_at: float | None
    updated_at: float
    last_error: str | None
    result: str | None


@dataclass(frozen=True)
class DispatchOutcome:
    """投递结果：要么新建，要么幂等命中已有任务。"""

    accepted: bool
    task_id: int
    state: TaskState
    duplicated: bool
