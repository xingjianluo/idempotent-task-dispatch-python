"""SQLite-backed task store.

All access goes through a single connection (check_same_thread=False) guarded
by an RLock, so concurrent dispatch/consumption from worker threads is safe
and every state transition is an atomic, row-guarded UPDATE.

State transitions are conditional on the expected previous state, which
prevents lost updates even if a handler call races with restart recovery.
"""
from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager

from .models import QueueCounters, TaskRecord, TaskState

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    business_key  TEXT NOT NULL UNIQUE,
    group_name    TEXT NOT NULL,
    payload_hash  TEXT NOT NULL,
    payload       TEXT NOT NULL,
    state         TEXT NOT NULL CHECK (state IN
                        ('pending','processing','succeeded','dead')),
    attempts      INTEGER NOT NULL DEFAULT 0,
    visible_after REAL NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_claim
    ON tasks(state, group_name, business_key, visible_after, id);
CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state);
"""


class TaskStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None

    # -- lifecycle -----------------------------------------------------------
    def init_schema(self) -> None:
        with self._lock:
            if self._conn is None:
                conn = sqlite3.connect(
                    self.db_path,
                    check_same_thread=False,
                    isolation_level=None,  # autocommit; explicit BEGIN/COMMIT
                    timeout=10.0,
                )
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA busy_timeout=10000")
                conn.execute("PRAGMA foreign_keys=ON")
                self._conn = conn
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Serialize a read+write block under one DB transaction."""
        if self._conn is None:
            raise RuntimeError("TaskStore.init_schema() must be called first")
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                yield self._conn
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def _db(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("TaskStore.init_schema() must be called first")
        return self._conn

    # -- mapping -------------------------------------------------------------
    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> TaskRecord:
        return TaskRecord(
            id=row["id"],
            business_key=row["business_key"],
            group_name=row["group_name"],
            payload_hash=row["payload_hash"],
            payload=row["payload"],
            state=TaskState(row["state"]),
            attempts=row["attempts"],
            visible_after=row["visible_after"],
            last_error=row["last_error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    # -- queries -------------------------------------------------------------
    def find_by_key(self, business_key: str) -> TaskRecord | None:
        with self._lock:
            row = self._db().execute(
                "SELECT * FROM tasks WHERE business_key = ?", (business_key,)
            ).fetchone()
        return None if row is None else self._row_to_record(row)

    def get(self, task_id: int) -> TaskRecord:
        with self._lock:
            row = self._db().execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
        if row is None:
            raise KeyError(f"task {task_id} not found")
        return self._row_to_record(row)

    def insert_pending(
        self,
        *,
        business_key: str,
        group_name: str,
        payload: str,
        payload_hash: str,
        now: float,
    ) -> int:
        with self._lock:
            cur = self._db().execute(
                """
                INSERT INTO tasks (business_key, group_name, payload_hash,
                                   payload, state, attempts, visible_after,
                                   last_error, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'pending', 0, 0, NULL, ?, ?)
                """,
                (business_key, group_name, payload_hash, payload, now, now),
            )
            return int(cur.lastrowid)

    def claim_head_task(self, now: float) -> TaskRecord | None:
        """Atomically claim the oldest visible pending head task.

        Head = the earliest pending task for its (group, key); a later task of
        the same key can never overtake an earlier one, even while the earlier
        one is waiting out its retry backoff. Different keys are independent
        and may be claimed concurrently by different workers.
        """
        with self.transaction() as conn:
            row = conn.execute(
                """
                SELECT * FROM tasks t
                WHERE t.state = 'pending'
                  AND t.visible_after <= ?
                  AND NOT EXISTS (
                      SELECT 1 FROM tasks e
                      WHERE e.group_name = t.group_name
                        AND e.business_key = t.business_key
                        AND e.state = 'pending'
                        AND e.id < t.id
                  )
                ORDER BY t.id
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                return None
            task_id = row["id"]
            conn.execute(
                "UPDATE tasks SET state='processing', updated_at=? WHERE id=?",
                (now, task_id),
            )
            return self._row_to_record(row)

    def mark_succeeded(self, task_id: int, *, attempts: int,
                       now: float) -> None:
        with self._lock:
            cur = self._db().execute(
                "UPDATE tasks SET state='succeeded', attempts=?, updated_at=? "
                "WHERE id=? AND state='processing'",
                (attempts, now, task_id),
            )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"task {task_id} is not in processing; cannot mark succeeded"
            )

    def mark_retry(self, task_id: int, *, attempts: int, visible_after: float,
                   error: str, now: float) -> None:
        with self._lock:
            cur = self._db().execute(
                "UPDATE tasks SET state='pending', attempts=?, "
                "visible_after=?, last_error=?, updated_at=? "
                "WHERE id=? AND state='processing'",
                (attempts, visible_after, error, now, task_id),
            )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"task {task_id} is not in processing; cannot schedule retry"
            )

    def mark_dead(self, task_id: int, *, attempts: int, error: str,
                  now: float) -> None:
        with self._lock:
            cur = self._db().execute(
                "UPDATE tasks SET state='dead', attempts=?, last_error=?, "
                "updated_at=? WHERE id=? AND state='processing'",
                (attempts, error, now, task_id),
            )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"task {task_id} is not in processing; cannot mark dead"
            )

    def requeue_stale_processing(self, now: float) -> list[int]:
        """On startup, tasks left in processing by a crashed process are put
        back to pending for immediate redelivery. Returns their ids."""
        with self.transaction() as conn:
            rows = conn.execute(
                "SELECT id FROM tasks WHERE state='processing'"
            ).fetchall()
            ids = [r["id"] for r in rows]
            if ids:
                conn.execute(
                    "UPDATE tasks SET state='pending', visible_after=?, "
                    "updated_at=? WHERE state='processing'",
                    (now, now),
                )
        return ids

    def counters(self) -> QueueCounters:
        with self._lock:
            rows = self._db().execute(
                "SELECT state, COUNT(*) AS n FROM tasks GROUP BY state"
            ).fetchall()
        counts = {r["state"]: r["n"] for r in rows}
        return QueueCounters(
            pending=counts.get("pending", 0),
            processing=counts.get("processing", 0),
            succeeded=counts.get("succeeded", 0),
            dead=counts.get("dead", 0),
        )

    def list_dead_letters(self) -> list[TaskRecord]:
        with self._lock:
            rows = self._db().execute(
                "SELECT * FROM tasks WHERE state='dead' ORDER BY id"
            ).fetchall()
        return [self._row_to_record(r) for r in rows]
