"""Local task dispatcher.

Responsibilities:

* **Idempotent dispatch** (see :meth:`Dispatcher.dispatch`): a business key is
  globally unique. Same key+payload is always a replay; same key with a new
  payload is rejected differently depending on whether the old task is still
  in flight (``key_occupied``) or already terminal (``content_conflict``).
* **Grouped ordered consumption**: each worker atomically claims the *head*
  task of a (group, business_key) FIFO. Different keys run concurrently, the
  same key strictly in order, and a backing-off task blocks later tasks of the
  same key until it becomes visible again.
* **Retry / dead-letter**: a failed or timed-out attempt is rescheduled after
  exponential backoff until ``max_attempts`` is exhausted; the task then goes
  to the dead-letter state with its last error preserved.

Time uses ``time.time()``; all durable state lives in SQLite, so a restart
requeues interrupted ``processing`` tasks and never re-runs confirmed ones.
"""
from __future__ import annotations

import hashlib
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Callable

from .errors import (
    ContentConflictError,
    InvalidRequestError,
    KeyOccupiedError,
)
from .models import DispatchResult, RetryPolicy, TaskRecord, TaskState
from .store import TaskStore

Handler = Callable[[TaskRecord], None]


class Dispatcher:
    MAX_KEY_LENGTH = 256
    MAX_GROUP_LENGTH = 128
    MAX_PAYLOAD_BYTES = 64 * 1024

    def __init__(
        self,
        store: TaskStore,
        handler: Handler,
        *,
        retry_policy: RetryPolicy | None = None,
        workers: int = 2,
        poll_interval: float = 0.05,
        logger: logging.Logger | None = None,
    ):
        if workers < 1:
            raise ValueError("workers must be >= 1")
        self.store = store
        self.handler = handler
        self.retry_policy = retry_policy or RetryPolicy()
        self.retry_policy.validate()
        self.workers = workers
        self.poll_interval = poll_interval
        self.logger = logger or logging.getLogger("idempotent_tasks")
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._worker_threads: list[threading.Thread] = []
        self._pool: ThreadPoolExecutor | None = None

    # -- lifecycle -----------------------------------------------------------
    def start(self) -> None:
        if self._worker_threads:
            return
        self._stop.clear()
        self._pool = ThreadPoolExecutor(
            max_workers=self.workers,
            thread_name_prefix="task-handler",
        )
        # Recover tasks a previous process left mid-processing.
        stale = self.store.requeue_stale_processing(time.time())
        for task_id in stale:
            self.logger.warning(
                "requeued stale processing task after restart task_id=%d",
                task_id,
                extra={"task_key": "-", "attempt": "-",
                       "decision": "restart_requeue"},
            )
        for i in range(self.workers):
            t = threading.Thread(
                target=self._worker_loop,
                name=f"task-worker-{i}",
                daemon=True,
            )
            t.start()
            self._worker_threads.append(t)

    def shutdown(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        for t in self._worker_threads:
            t.join(timeout=timeout)
        self._worker_threads.clear()
        if self._pool is not None:
            # Running handlers may finish; state transitions remain guarded.
            self._pool.shutdown(wait=True, cancel_futures=False)
            self._pool = None

    def wait_until_drained(self, timeout: float = 10.0) -> bool:
        """Wait until no task is pending/processing. Returns False on timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            c = self.store.counters()
            if c.pending == 0 and c.processing == 0:
                return True
            time.sleep(0.01)
        return False

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            task = self.store.claim_head_task(time.time())
            if task is None:
                self._wake.wait(self.poll_interval)
                self._wake.clear()
                continue
            self._wake.clear()
            self.logger.info(
                "claimed task task_id=%d group=%s",
                task.id, task.group_name,
                extra={"task_key": task.business_key,
                       "attempt": task.attempts + 1,
                       "decision": "claimed"},
            )
            self._process(task)

    # -- dispatch ------------------------------------------------------------
    def dispatch(
        self,
        business_key: str,
        *,
        group_name: str = "default",
        payload: str = "",
    ) -> DispatchResult:
        self._validate(business_key, group_name, payload)
        payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        now = time.time()

        with self.store.transaction():
            existing = self.store.find_by_key(business_key)
            if existing is None:
                task_id = self.store.insert_pending(
                    business_key=business_key,
                    group_name=group_name,
                    payload=payload,
                    payload_hash=payload_hash,
                    now=now,
                )
                result = DispatchResult(
                    accepted=True, task_id=task_id,
                    state=TaskState.PENDING, reason="accepted",
                )
                decision = "accepted"
            else:
                result, decision = self._judge_replay(existing, payload_hash)

        self.logger.info(
            "dispatch judged task_id=%d state=%s",
            result.task_id, result.state.value,
            extra={"task_key": business_key, "attempt": 0,
                   "decision": decision},
        )
        self._wake.set()
        return result

    @staticmethod
    def _judge_replay(existing: TaskRecord, payload_hash: str
                      ) -> tuple[DispatchResult, str]:
        same_payload = existing.payload_hash == payload_hash
        if existing.state.terminal:
            if same_payload:
                # Confirmed work is never re-executed; the terminal task is
                # returned idempotently whether it succeeded or is dead.
                reason = (
                    "replay_succeeded"
                    if existing.state is TaskState.SUCCEEDED
                    else "replay_dead"
                )
                return (
                    DispatchResult(False, existing.id, existing.state, reason),
                    reason,
                )
            raise ContentConflictError(
                f"business key {existing.business_key!r} already finished "
                f"(state={existing.state.value}) with a different payload; "
                "reusing the key with new content is not allowed",
                existing_task_id=existing.id,
            )

        # In flight (pending/processing).
        if same_payload:
            return (
                DispatchResult(False, existing.id, existing.state,
                               "duplicate_in_flight"),
                "duplicate_in_flight",
            )
        raise KeyOccupiedError(
            f"business key {existing.business_key!r} is occupied by task "
            f"{existing.id} in state {existing.state.value}; wait for it to "
            "finish before dispatching different content under the same key",
            existing_task_id=existing.id,
        )

    def _validate(self, business_key: object, group_name: object,
                  payload: object) -> None:
        def bad(msg: str) -> None:
            raise InvalidRequestError(msg)

        if not isinstance(business_key, str) or not business_key.strip():
            bad("business_key must be a non-empty string")
        if len(business_key) > self.MAX_KEY_LENGTH:
            bad(f"business_key must be <= {self.MAX_KEY_LENGTH} chars")
        if not isinstance(group_name, str) or not group_name.strip():
            bad("group_name must be a non-empty string")
        if len(group_name) > self.MAX_GROUP_LENGTH:
            bad(f"group_name must be <= {self.MAX_GROUP_LENGTH} chars")
        if not isinstance(payload, str):
            bad("payload must be a string")
        if len(payload.encode("utf-8")) > self.MAX_PAYLOAD_BYTES:
            bad(f"payload must be <= {self.MAX_PAYLOAD_BYTES} bytes")

    # -- processing ----------------------------------------------------------
    def _process(self, task: TaskRecord) -> None:
        attempt = task.attempts + 1
        ok, error = self._run_handler(task)
        now = time.time()
        try:
            if ok:
                self.store.mark_succeeded(task.id, attempts=attempt, now=now)
                self.logger.info(
                    "task succeeded task_id=%d after %d attempt(s)",
                    task.id, attempt,
                    extra={"task_key": task.business_key, "attempt": attempt,
                           "decision": "succeeded"},
                )
                return

            if attempt >= self.retry_policy.max_attempts:
                self.store.mark_dead(
                    task.id, attempts=attempt, error=error, now=now,
                )
                self.logger.error(
                    "task exhausted retries, dead-lettered task_id=%d "
                    "attempts=%d error=%s",
                    task.id, attempt, error,
                    extra={"task_key": task.business_key, "attempt": attempt,
                           "decision": "dead"},
                )
                return

            delay = self.retry_policy.backoff_for(attempt)
            self.store.mark_retry(
                task.id, attempts=attempt,
                visible_after=now + delay, error=error, now=now,
            )
            self.logger.warning(
                "task failed, scheduling retry task_id=%d attempt=%d "
                "delay=%.3fs error=%s",
                task.id, attempt, delay, error,
                extra={"task_key": task.business_key, "attempt": attempt,
                       "decision": "retry"},
            )
            # Workers poll every poll_interval, so a backing-off task becomes
            # visible at most one poll interval after its delay expires; no
            # extra timer threads are needed.
        except RuntimeError as exc:
            # The guarded transition lost (row no longer processing, e.g.
            # restart recovery moved it). Nothing is stuck: log and move on.
            self.logger.error(
                "state already changed, skipping transition task_id=%d: %s",
                task.id, exc,
                extra={"task_key": task.business_key, "attempt": attempt,
                       "decision": "state_changed"},
            )

    def _run_handler(self, task: TaskRecord) -> tuple[bool, str | None]:
        assert self._pool is not None, "Dispatcher.start() must be called first"
        future = self._pool.submit(self.handler, task)
        timeout = self.retry_policy.timeout
        try:
            future.result(timeout=timeout)
            return True, None
        except TimeoutError:
            # The worker thread cannot be killed; the handler keeps running in
            # the background but its eventual result is discarded. The durable
            # state moves on, so a commit (succeeded/dead) is never duplicated.
            return False, (
                f"handler timed out after {timeout}s on attempt "
                f"{task.attempts + 1}"
            )
        except Exception as exc:  # handler itself raised
            return False, f"{type(exc).__name__}: {exc}"
