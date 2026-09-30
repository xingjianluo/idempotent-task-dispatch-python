"""Exception hierarchy so callers can distinguish rejection reasons."""
from __future__ import annotations

from .models import RejectReason


class DispatchError(Exception):
    """Base class for all dispatch rejections."""

    reason: RejectReason = RejectReason.INVALID_REQUEST


class InvalidRequestError(DispatchError):
    """Malformed request: missing/blank key, group or over-long payload."""

    reason = RejectReason.INVALID_REQUEST


class KeyOccupiedError(DispatchError):
    """The business key is currently held by a non-terminal task
    (pending/processing) whose payload differs from this request."""

    reason = RejectReason.KEY_OCCUPIED

    def __init__(self, message: str, *, existing_task_id: int):
        super().__init__(message)
        self.existing_task_id = existing_task_id


class ContentConflictError(DispatchError):
    """The business key has a terminal task (succeeded/dead) with a different
    payload hash; reusing the key with new content is a conflict."""

    reason = RejectReason.CONTENT_CONFLICT

    def __init__(self, message: str, *, existing_task_id: int):
        super().__init__(message)
        self.existing_task_id = existing_task_id
