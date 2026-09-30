"""分组消费者：同键顺序、异键并发、超时租约、退避重试与死信。

顺序与并发约定
    - 一个 Worker 绑定一个 group；同组内同一个 key 的任务在同一时刻
      只会有一个 RUNNING（UNIQUE(grp,key) + 状态机保证），因此天然按
      “每次投递一个任务”的方式顺序处理；不同 key 通过线程池并发。
    - 处理超时（lease_seconds）的任务会在 pump 中被收回 PENDING 并重新
      领取；原执行者迟到的 complete/requeue/mark_dead 因 CAS（attempts
      不匹配）失败，不会重复生效 —— 语义上是“至少一次”，失败原因只记一次。

数量守恒
    任意时刻任务只处于 PENDING / RUNNING / SUCCEEDED / DEAD 之一；
    失败后要么 requeue 回 PENDING，要么（最后一次尝试）mark_dead 进死信，
    不存在“既没完成也没进死信”的中间态。

重启
    start() 会先 recover(force_all=True)：上一进程遗留的 RUNNING 全部退回
    PENDING（attempts 保留）。已 SUCCEEDED/DEAD 的任务不受影响、不会再执行。
"""
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime

from .models import TaskRecord
from .service import default_backoff
from .store import TaskStore

logger = logging.getLogger("taskdispatch.worker")

Handler = Callable[[TaskRecord], str]


class Worker:
    def __init__(
        self,
        store: TaskStore,
        group: str,
        handler: Handler,
        *,
        concurrency: int = 4,
        lease_seconds: float = 30.0,
        poll_interval: float = 0.02,
        backoff: Callable[[int], float] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self.store = store
        self.group = group
        self.handler = handler
        self.concurrency = concurrency
        self.lease_seconds = lease_seconds
        self.poll_interval = poll_interval
        self.backoff = backoff or default_backoff
        self.clock = clock
        self.sleep = sleep

        self._pool = ThreadPoolExecutor(
            max_workers=concurrency, thread_name_prefix=f"worker-{group}"
        )
        self._futs: set[Future] = set()
        # task_id -> 该任务最早一个超时但尚未结束的 handler 线程。
        # 只要任务还有旧 handler 线程在跑，就不允许再次领取，
        # 否则超时任务会被连续重领，导致 attempts 超过 max_attempts。
        self._zombie_threads: dict[int, threading.Thread] = {}
        self._state_lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ---------- 生命周期 ----------

    def start(self) -> None:
        if self._thread is not None:
            return
        self.recover(force_all=True)
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name=f"pump-{self.group}", daemon=True
        )
        self._thread.start()
        logger.info("worker started group=%s concurrency=%s", self.group, self.concurrency)

    def stop(self, wait: bool = True) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None and wait:
            self._thread.join(timeout=5.0)
            self._thread = None
        # 注意：不 shutdown 线程池里正在执行的 handler；未完成任务靠租约/重启恢复
        self._pool.shutdown(wait=wait, cancel_futures=False)

    def recover(self, *, force_all: bool = False) -> int:
        """把遗留 RUNNING 收回 PENDING。返回收回数量。

        force_all=True 用于重启（不信任旧租约时钟）；
        force_all=False 仅收回 leased_at 早于 now-lease_seconds 的超时任务。
        """
        now = self.clock()
        leased_before = None if force_all else now - self.lease_seconds
        n = self.store.recover_expired(
            now, group=self.group, leased_before=leased_before
        )
        if n:
            logger.warning(
                "worker recover group=%s reclaimed=%s reason=%s",
                self.group, n, "restart" if force_all else "lease_timeout",
            )
        return n

    def run_until_idle(self, timeout: float | None = None) -> None:
        """跑到本组既无 RUNNING（含在途 future）也无 PENDING 为止。"""
        deadline = None if timeout is None else self.clock() + timeout
        self.recover(force_all=False)
        while True:
            self._pump()
            with self._state_lock:
                busy = self._inflight_locked() > 0
            earliest = self.store.earliest_pending(self.group)
            now = self.clock()
            if not busy and earliest is None:
                return
            if deadline is not None and now >= deadline:
                raise TimeoutError(
                    f"group {self.group!r} not idle before timeout"
                )
            wait_s = self.poll_interval
            if earliest is not None:
                wait_s = max(wait_s, min(0.5, earliest - now))
            self.sleep(wait_s)

    # ---------- 调度循环 ----------

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._pump()
            self._wake.wait(timeout=self.poll_interval)
            self._wake.clear()

    def _pump(self) -> None:
        now = self.clock()
        # 定时收回超时租约（已完成的 future 的 CAS 会自动失效，不会被误算）
        reclaimed = self.store.recover_expired(
            now, group=self.group, leased_before=now - self.lease_seconds
        )
        if reclaimed:
            logger.warning(
                "lease expired group=%s reclaimed=%s attempts=pending reason=lease_timeout",
                self.group, reclaimed,
            )

        with self._state_lock:
            in_flight = self._inflight_locked()
        free = self.concurrency - in_flight
        for _ in range(max(0, free)):
            record = self.store.acquire_next(
                self.group, now=now, lease_seconds=self.lease_seconds
            )
            if record is None:
                break
            if record.id in self._zombie_threads:
                # 该任务旧尝试的 handler 线程还没结束，放回短退避，避免并发两份
                # 以及 attempts 被超时重领推过 max_attempts
                self.store.requeue(
                    record.id,
                    record.attempts,
                    error="previous attempt still terminating",
                    available_at=now + self.poll_interval * 5,
                    now=now,
                )
                continue
            fut = self._pool.submit(self._process, record)
            with self._state_lock:
                self._futs.add(fut)
            fut.add_done_callback(self._future_done)

        # 回收已结束的 future；只有 handler 线程真正结束后才解除 zombie 排除
        done = {f for f in self._futs if f.done()}
        if done:
            with self._state_lock:
                for f in done:
                    self._futs.discard(f)
                    ex = f.exception()
                    if ex is not None:
                        logger.error("worker future crashed group=%s error=%r",
                                     self.group, ex)
        self._cleanup_zombies()

    def _cleanup_zombies(self) -> None:
        with self._state_lock:
            for tid in [
                tid for tid, th in self._zombie_threads.items() if not th.is_alive()
            ]:
                del self._zombie_threads[tid]
                logger.info(
                    "stale handler finished group=%s task_id=%s reason=zombie_thread_ended",
                    self.group, tid,
                )

    def _future_done(self, fut: Future) -> None:
        self._wake.set()

    def _inflight_locked(self) -> int:
        """调用方需持有 _state_lock。未结束的 future 数（含僵尸执行）。"""
        return sum(1 for f in self._futs if not f.done())

    # ---------- 单次处理 ----------

    def _process(self, record: TaskRecord) -> None:
        ok, result, error = self._run_handler(record)
        now = self.clock()

        if ok:
            applied = self.store.complete(record.id, record.attempts, result, now)
            logger.info(
                "task finished group=%s key=%s task_id=%s attempts=%s/%s applied=%s "
                "reason=%s",
                record.group, record.key, record.id, record.attempts,
                record.max_attempts, applied,
                "succeeded" if applied else "stale_attempt_ignored",
            )
            return

        if record.attempts >= record.max_attempts:
            reason = f"retries exhausted after {record.attempts} attempts: {error}"
            applied = self.store.mark_dead(record.id, record.attempts, reason, now)
            logger.error(
                "task dead group=%s key=%s task_id=%s attempts=%s/%s applied=%s "
                "reason=retries_exhausted last_error=%r",
                record.group, record.key, record.id, record.attempts,
                record.max_attempts, applied, error,
            )
            return

        delay = self.backoff(record.attempts)
        available_at = now + delay
        applied = self.store.requeue(
            record.id, record.attempts, error, available_at=available_at, now=now
        )
        logger.warning(
            "task retry group=%s key=%s task_id=%s attempts=%s/%s applied=%s "
            "retry_in=%.3f reason=handler_failed last_error=%r",
            record.group, record.key, record.id, record.attempts,
            record.max_attempts, applied, delay, error,
        )

    def _run_handler(self, record: TaskRecord) -> tuple[bool, str, str]:
        """在独立线程里执行 handler 并施加租约超时。返回 (ok, result, error)。"""
        box: dict = {}

        def target() -> None:
            try:
                box["result"] = self.handler(record)
            except Exception as exc:  # noqa: BLE001 - 业务异常统一视为失败
                box["error"] = f"{type(exc).__name__}: {exc}"

        th = threading.Thread(
            target=target,
            name=f"handler-{record.id}-{record.attempts}",
            daemon=True,
        )
        th.start()
        th.join(self.lease_seconds)
        if th.is_alive():
            with self._state_lock:
                # 只保留最早未结束的那个线程用于判断“该任务是否可再次领取”
                self._zombie_threads.setdefault(record.id, th)
            logger.warning(
                "task timeout group=%s key=%s task_id=%s attempts=%s/%s "
                "reason=lease_timeout lease=%.1fs",
                record.group, record.key, record.id, record.attempts,
                record.max_attempts, self.lease_seconds,
            )
            return False, "", f"timeout after {self.lease_seconds}s"
        if "error" in box:
            return False, "", box["error"]
        result = box.get("result")
        if not isinstance(result, str):
            result = str(result)
        return True, result, ""

    @staticmethod
    def _iso(ts: float | None) -> str:
        if ts is None:
            return ""
        return datetime.fromtimestamp(ts, UTC).isoformat()
