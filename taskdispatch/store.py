"""SQLite 任务存储：所有状态流转都在短事务里完成。

线程模型：单个连接 + 进程内 RLock，配合 SQLite 的事务语义，
保证投递、领取、确认、重试、进死信并发发生时数量守恒。

所有状态流转都带 CAS 条件（WHERE state=? AND attempts=?）：
即便某次执行超时被收回、之后又被另一个消费者重新领取，
旧执行者迟到的确认/重试/进死信也不会覆盖新状态。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager

from .models import TaskRecord, TaskState

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    grp          TEXT NOT NULL,
    key          TEXT NOT NULL,
    payload      TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    state        TEXT NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    available_at REAL NOT NULL,
    leased_at    REAL,
    updated_at   REAL NOT NULL,
    last_error   TEXT,
    result       TEXT,
    UNIQUE(grp, key)
);
CREATE INDEX IF NOT EXISTS idx_pick ON tasks(grp, state, available_at);
"""


class TaskStore:
    def __init__(self, path: str = ":memory:"):
        self._path = path
        if path != ":memory:":
            parent = os.path.dirname(os.path.abspath(path))
            os.makedirs(parent, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout=5000")
        if path != ":memory:":
            # 文件库开 WAL，保证进程异常退出后已 COMMIT 的状态依然落盘
            self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def exclusive(self):
        """把“读后判定再写”包成原子段（RLock 可与内部短事务嵌套）。"""
        with self._lock:
            yield

    # ---------- 投递侧 ----------

    def insert_pending(
        self,
        group: str,
        key: str,
        payload: str,
        payload_hash: str,
        available_at: float,
        max_attempts: int,
    ) -> int:
        with self._lock, self._tx() as cur:
            cur.execute(
                """
                INSERT INTO tasks(grp, key, payload, payload_hash, state,
                                  attempts, max_attempts, available_at,
                                  leased_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 0, ?, ?, NULL, ?)
                """,
                (
                    group,
                    key,
                    payload,
                    payload_hash,
                    TaskState.PENDING.value,
                    max_attempts,
                    available_at,
                    available_at,
                ),
            )
            return int(cur.lastrowid)

    def get_by_key(self, group: str, key: str) -> TaskRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE grp=? AND key=?", (group, key)
            ).fetchone()
            return self._to_record(row) if row is not None else None

    def get_by_id(self, task_id: int) -> TaskRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            return self._to_record(row) if row is not None else None

    def count_by_state(self, group: str | None = None) -> dict[str, int]:
        sql = "SELECT state, COUNT(*) AS n FROM tasks"
        args: tuple = ()
        if group is not None:
            sql += " WHERE grp=?"
            args = (group,)
        sql += " GROUP BY state"
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        counts = {s.value: 0 for s in TaskState}
        for row in rows:
            counts[row["state"]] = row["n"]
        return counts

    # ---------- 消费侧 ----------

    def acquire_next(
        self, group: str, now: float, lease_seconds: float
    ) -> TaskRecord | None:
        """原子地领取一条到期的 PENDING 任务：PENDING -> RUNNING，attempts+1。"""
        with self._lock, self._tx() as cur:
            row = cur.execute(
                """
                SELECT * FROM tasks
                WHERE grp=? AND state=? AND available_at<=?
                ORDER BY available_at, id
                LIMIT 1
                """,
                (group, TaskState.PENDING.value, now),
            ).fetchone()
            if row is None:
                return None
            task_id = row["id"]
            next_attempts = row["attempts"] + 1
            cur.execute(
                """
                UPDATE tasks
                SET state=?, attempts=?, leased_at=?, updated_at=?
                WHERE id=? AND state=?
                """,
                (
                    TaskState.RUNNING.value,
                    next_attempts,
                    now,
                    now,
                    task_id,
                    TaskState.PENDING.value,
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError("acquire_next: row vanished between select and update")
            updated = cur.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            return self._to_record(updated)

    def complete(self, task_id: int, attempt: int, result: str, now: float) -> bool:
        """RUNNING -> SUCCEEDED。CAS 要求 attempts 与领取时一致，陈旧执行者返回 False。"""
        with self._lock, self._tx() as cur:
            cur.execute(
                """
                UPDATE tasks
                SET state=?, result=?, last_error=NULL,
                    leased_at=NULL, updated_at=?
                WHERE id=? AND state=? AND attempts=?
                """,
                (
                    TaskState.SUCCEEDED.value,
                    result,
                    now,
                    task_id,
                    TaskState.RUNNING.value,
                    attempt,
                ),
            )
            return cur.rowcount == 1

    def requeue(
        self, task_id: int, attempt: int, error: str, available_at: float, now: float
    ) -> bool:
        """RUNNING -> PENDING（退避后到期）。陈旧执行者返回 False。"""
        with self._lock, self._tx() as cur:
            cur.execute(
                """
                UPDATE tasks
                SET state=?, leased_at=NULL, last_error=?,
                    available_at=?, updated_at=?
                WHERE id=? AND state=? AND attempts=?
                """,
                (
                    TaskState.PENDING.value,
                    error,
                    available_at,
                    now,
                    task_id,
                    TaskState.RUNNING.value,
                    attempt,
                ),
            )
            return cur.rowcount == 1

    def mark_dead(self, task_id: int, attempt: int, reason: str, now: float) -> bool:
        """RUNNING -> DEAD（重试耗尽）。陈旧执行者返回 False。"""
        with self._lock, self._tx() as cur:
            cur.execute(
                """
                UPDATE tasks
                SET state=?, leased_at=NULL, last_error=?, updated_at=?
                WHERE id=? AND state=? AND attempts=?
                """,
                (
                    TaskState.DEAD.value,
                    reason,
                    now,
                    task_id,
                    TaskState.RUNNING.value,
                    attempt,
                ),
            )
            return cur.rowcount == 1

    def recover_expired(
        self,
        now: float,
        group: str | None = None,
        *,
        leased_before: float | None = None,
    ) -> int:
        """把 RUNNING 任务退回 PENDING。

        - leased_before=None：收回全部 RUNNING（重启场景，租约时钟不可信）。
        - leased_before=ts ：只收回 leased_at < ts 的（运行中超时场景）。
        退避进度不重置（attempts 保留），available_at 设为 now 立即重新参与调度。
        """
        sql = "UPDATE tasks SET state=?, leased_at=NULL, available_at=?, updated_at=? WHERE state=?"
        args: list = [TaskState.PENDING.value, now, now, TaskState.RUNNING.value]
        if leased_before is not None:
            sql += " AND leased_at<?"
            args.append(leased_before)
        if group is not None:
            sql += " AND grp=?"
            args.append(group)
        with self._lock, self._tx() as cur:
            cur.execute(sql, args)
            return cur.rowcount

    def earliest_pending(self, group: str) -> float | None:
        """返回该分组最早一条 PENDING 任务的到期时间；没有则 None。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT MIN(available_at) AS t FROM tasks WHERE grp=? AND state=?",
                (group, TaskState.PENDING.value),
            ).fetchone()
        t = row["t"] if row is not None else None
        return float(t) if t is not None else None

    def list_dead(self, group: str | None = None) -> list[TaskRecord]:
        sql = "SELECT * FROM tasks WHERE state=?"
        args: tuple = (TaskState.DEAD.value,)
        if group is not None:
            sql += " AND grp=?"
            args = (TaskState.DEAD.value, group)
        sql += " ORDER BY id"
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [self._to_record(row) for row in rows]

    # ---------- 内部 ----------

    def _to_record(self, row: sqlite3.Row) -> TaskRecord:
        return TaskRecord(
            id=row["id"],
            group=row["grp"],
            key=row["key"],
            payload=row["payload"],
            payload_hash=row["payload_hash"],
            state=TaskState(row["state"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            available_at=row["available_at"],
            leased_at=row["leased_at"],
            updated_at=row["updated_at"],
            last_error=row["last_error"],
            result=row["result"],
        )

    class _Tx:
        def __init__(self, conn: sqlite3.Connection):
            self._conn = conn

        def __enter__(self) -> sqlite3.Cursor:
            self._conn.execute("BEGIN IMMEDIATE")
            return self._conn.cursor()

        def __exit__(self, exc_type, exc, tb) -> None:
            if exc_type is None:
                self._conn.execute("COMMIT")
            else:
                self._conn.execute("ROLLBACK")

    def _tx(self) -> "TaskStore._Tx":
        return TaskStore._Tx(self._conn)
