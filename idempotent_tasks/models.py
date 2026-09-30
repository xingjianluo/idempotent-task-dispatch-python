"""Core domain types: task states, reject reasons, configuration and value objects."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class TaskState(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    DEAD = "dead"

    @property
    def terminal(self) -> bool:
        return self in (TaskState.SUCCEEDED, TaskState.DEAD)


class RejectReason(str, Enum):
    INVALID_REQUEST = "invalid_request"
    KEY_OCCUPIED = "key_occupied"
    CONTENT_CONFLICT = "content_conflict"


@dataclass(frozen=True)
class RetryPolicy:
    """Retry configuration.

    A task may be attempted up to ``max_attempts`` times. After failed attempt
    n (1-indexed) the task becomes visible again after

        delay = min(max_delay, base_delay * factor ** (n - 1)).

    ``timeout`` is the per-attempt wall-clock limit; ``None`` disables it.
    """

    max_attempts: int = 3
    base_delay: float = 1.0
    factor: float = 2.0
    max_delay: float = 30.0
    timeout: float | None = 5.0

    def backoff_for(self, attempt: int) -> float:
        # Attempt just consumed is `attempt`; it was retry number (attempt-1).
        raw = self.base_delay * (self.factor ** max(0, attempt - 1))
        return min(self.max_delay, raw)

    def validate(self) -> None:
        if not isinstance(self.max_attempts, int) or self.max_attempts < 1:
            raise ValueError("max_attempts must be an int >= 1")
        if self.base_delay < 0:
            raise ValueError("base_delay must be >= 0")
        if self.factor < 1:
            raise ValueError("factor must be >= 1")
        if self.max_delay < 0:
            raise ValueError("max_delay must be >= 0")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be > 0 or None")


@dataclass(frozen=True)
class TaskRecord:
    id: int
    business_key: str
    group_name: str
    payload_hash: str
    payload: str
    state: TaskState
    attempts: int
    visible_after: float
    last_error: str | None
    created_at: float
    updated_at: float


@dataclass(frozen=True)
class DispatchResult:
    """Outcome of a dispatch call.

    accepted=True means the task was newly created.
    accepted=False means an idempotent replay: an existing terminal task was
    found for the same key+payload (reason: replay_succeeded/replay_dead).
    """

    accepted: bool
    task_id: int
    state: TaskState
    reason: str | None = None


@dataclass(frozen=True)
class QueueCounters:
    pending: int
    processing: int
    succeeded: int
    dead: int

    @property
    def total(self) -> int:
        return self.pending + self.processing + self.succeeded + self.dead
