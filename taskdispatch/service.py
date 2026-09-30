"""投递服务：参数校验、幂等判定与冲突区分。

判定顺序（同一个 (group, key)）：
    1. 请求不合法（键/分组为空、载荷为 None、max_attempts<1） -> InvalidRequestError
    2. 已存在且载荷哈希不同                                    -> PayloadConflictError
    3. 载荷相同但任务在途（PENDING/RUNNING）                   -> KeyOccupiedError
    4. 载荷相同且已终态（SUCCEEDED/DEAD）                      -> 幂等返回，不重复执行
    5. 不存在                                                 -> 新建 PENDING
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable

from .exceptions import (
    InvalidRequestError,
    KeyOccupiedError,
    PayloadConflictError,
)
from .models import DispatchOutcome, TaskState
from .store import TaskStore

logger = logging.getLogger("taskdispatch.service")

# (attempts_so_far) -> 下一次重试前的退避秒数，便于测试覆盖
BackoffFn = Callable[[int], float]

_TERMINAL = (TaskState.SUCCEEDED, TaskState.DEAD)


def default_backoff(attempts: int) -> float:
    """指数退避：1s, 2s, 4s, ... 封顶 60s。attempts 为已经失败的次数。"""
    return min(60.0, 2.0 ** max(0, attempts - 1))


class TaskService:
    def __init__(
        self,
        store: TaskStore,
        *,
        max_attempts: int = 3,
        backoff: BackoffFn | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.store = store
        self.max_attempts = max_attempts
        self.backoff = backoff or default_backoff
        self.clock = clock

    def dispatch(
        self,
        group: str,
        key: str,
        payload: object,
        *,
        max_attempts: int | None = None,
    ) -> DispatchOutcome:
        """投递任务。异常与返回语义见模块文档字符串。"""
        self._validate(group, key, payload, max_attempts)
        attempts_cap = self.max_attempts if max_attempts is None else max_attempts

        encoded = self.encode_payload(payload)
        digest = self.hash_payload(encoded)
        now = self.clock()

        # “查-判-写”必须是原子段，否则并发同键投递会撞 UNIQUE 约束
        with self.store.exclusive():
            return self._decide(group, key, encoded, digest, now, attempts_cap)

    def _decide(
        self,
        group: str,
        key: str,
        encoded: str,
        digest: str,
        now: float,
        attempts_cap: int,
    ) -> DispatchOutcome:
        existing = self.store.get_by_key(group, key)
        if existing is None:
            task_id = self.store.insert_pending(
                group, key, encoded, digest, available_at=now, max_attempts=attempts_cap
            )
            logger.info(
                "dispatch accepted group=%s key=%s task_id=%s reason=new",
                group, key, task_id,
            )
            return DispatchOutcome(
                accepted=True, task_id=task_id, state=TaskState.PENDING, duplicated=False
            )

        if existing.payload_hash != digest:
            logger.warning(
                "dispatch rejected group=%s key=%s task_id=%s attempts=%s reason=payload_conflict",
                group, key, existing.id, existing.attempts,
            )
            raise PayloadConflictError(key, group)

        if existing.state in _TERMINAL:
            # 幂等重放：已确认过的任务不会再跑第二遍（重启后亦然）
            logger.info(
                "dispatch accepted group=%s key=%s task_id=%s attempts=%s "
                "state=%s reason=idempotent_replay",
                group, key, existing.id, existing.attempts, existing.state.value,
            )
            return DispatchOutcome(
                accepted=True,
                task_id=existing.id,
                state=existing.state,
                duplicated=True,
            )

        logger.warning(
            "dispatch rejected group=%s key=%s task_id=%s attempts=%s "
            "state=%s reason=key_occupied",
            group, key, existing.id, existing.attempts, existing.state.value,
        )
        raise KeyOccupiedError(key, group)

    @staticmethod
    def hash_payload(payload: str) -> str:
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def encode_payload(payload: object) -> str:
        try:
            return json.dumps(
                payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")
            )
        except (TypeError, ValueError) as exc:
            raise InvalidRequestError(f"payload is not JSON serializable: {exc}") from exc

    @staticmethod
    def _validate(
        group: object, key: object, payload: object, max_attempts: int | None
    ) -> None:
        if not isinstance(group, str) or not group.strip():
            raise InvalidRequestError("group must be a non-empty string")
        if not isinstance(key, str) or not key.strip():
            raise InvalidRequestError("key must be a non-empty string")
        if payload is None:
            raise InvalidRequestError("payload must not be null")
        if max_attempts is not None and (
            not isinstance(max_attempts, int) or max_attempts < 1
        ):
            raise InvalidRequestError("max_attempts must be a positive integer")
