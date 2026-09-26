"""Persistence layer backed by the standard-library ``sqlite3`` module.

A single file database gives us durable, restart-surviving state with zero
external dependencies. The store exposes an explicit state-transition API
(compare-and-set) so that the service layer can never silently move a job
backwards or execute it twice.

Concurrency model: a single sqlite connection guarded by a ``threading.Lock``.
Writes run inside ``BEGIN IMMEDIATE`` transactions; the compare-and-set
``UPDATE ... WHERE state IN (...)`` statement is the authoritative guard
against double execution / state rollback, even across dispatcher races.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any, Iterable

from .models import ErrorClass, Job, JobState

# Column order shared by all read paths.
_COLUMNS = (
    "id", "idempotency_key", "payload_hash", "job_type", "payload",
    "max_attempts", "state", "error_class", "error_message", "result",
    "attempt", "created_at", "updated_at", "enqueued_at", "not_before",
    "deadline_queue", "started_at", "finished_at", "attempt_history",
)


class JobStore:
    def __init__(self, db_path: str = ":memory:") -> None:
        self._db_path = db_path
        self._lock = threading.RLock()
        self.closed = False
        self._conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
            isolation_level=None,  # we manage transactions explicitly
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._create_schema()

    def close(self) -> None:
        with self._lock:
            if self.closed:
                return
            self.closed = True
            self._conn.close()

    # ------------------------------------------------------------------ #
    # schema
    # ------------------------------------------------------------------ #

    def _create_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id              TEXT PRIMARY KEY,
                    idempotency_key TEXT UNIQUE,
                    payload_hash    TEXT NOT NULL,
                    job_type        TEXT NOT NULL,
                    payload         TEXT NOT NULL,
                    max_attempts    INTEGER NOT NULL,
                    state           TEXT NOT NULL,
                    error_class     TEXT NOT NULL,
                    error_message   TEXT,
                    result          TEXT,
                    attempt         INTEGER NOT NULL,
                    created_at      REAL NOT NULL,
                    updated_at      REAL NOT NULL,
                    enqueued_at     REAL,
                    not_before      REAL NOT NULL DEFAULT 0,
                    deadline_queue  REAL,
                    started_at      REAL,
                    finished_at     REAL,
                    attempt_history TEXT NOT NULL DEFAULT '[]'
                );
                CREATE INDEX IF NOT EXISTS idx_jobs_dispatch
                    ON jobs (state, not_before, created_at, id);

                CREATE TABLE IF NOT EXISTS runtime_meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

    # ------------------------------------------------------------------ #
    # shutdown marker: distinguishes orderly shutdown from hard crash
    # ------------------------------------------------------------------ #

    def mark_orderly_shutdown(self, now: float) -> None:
        """Persist that this process stopped its runner deliberately."""
        if self.closed:
            return
        with self._lock:
            self._conn.execute(
                "INSERT INTO runtime_meta(key, value) VALUES('shutdown', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(now),),
            )

    def clear_shutdown_marker(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM runtime_meta WHERE key = 'shutdown'")

    def is_orderly_shutdown(self) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM runtime_meta WHERE key = 'shutdown'"
            ).fetchone()
        return row is not None

    # ------------------------------------------------------------------ #
    # row mapping
    # ------------------------------------------------------------------ #

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> Job:
        return Job(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            payload_hash=row["payload_hash"],
            job_type=row["job_type"],
            payload=json.loads(row["payload"]),
            max_attempts=row["max_attempts"],
            state=JobState(row["state"]),
            error_class=ErrorClass(row["error_class"]),
            error_message=row["error_message"],
            result=None if row["result"] is None else json.loads(row["result"]),
            attempt=row["attempt"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            enqueued_at=row["enqueued_at"],
            not_before=row["not_before"],
            deadline_queue=row["deadline_queue"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            attempt_history=json.loads(row["attempt_history"]),
        )

    def _fetch_one(self, sql: str, params: tuple[Any, ...]) -> Job | None:
        with self._lock:
            row = self._conn.execute(sql, params).fetchone()
        return None if row is None else self._row_to_job(row)

    # ------------------------------------------------------------------ #
    # read side
    # ------------------------------------------------------------------ #

    def get(self, job_id: str) -> Job | None:
        return self._fetch_one("SELECT * FROM jobs WHERE id = ?", (job_id,))

    def find_idempotent(self, idempotency_key: str) -> Job | None:
        return self._fetch_one(
            "SELECT * FROM jobs WHERE idempotency_key = ?", (idempotency_key,)
        )

    def list_due(self, now: float, limit: int) -> list[Job]:
        """Jobs ready to dispatch, oldest first.

        Includes QUEUED jobs (created order) and AWAITING_RETRY jobs whose
        backoff has elapsed. Queue-deadline expiry is handled separately by
        :meth:`expire_queue_timeouts`.
        """
        sql = (
            "SELECT * FROM jobs "
            "WHERE (state = 'QUEUED' "
            "       OR (state = 'AWAITING_RETRY' AND not_before <= ?)) "
            "ORDER BY not_before ASC, created_at ASC, id ASC "
            "LIMIT ?"
        )
        with self._lock:
            rows = self._conn.execute(sql, (now, limit)).fetchall()
        return [self._row_to_job(r) for r in rows]

    def count_queue_waiters(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS c FROM jobs WHERE state = 'QUEUED'"
            ).fetchone()
        return int(row["c"])

    # ------------------------------------------------------------------ #
    # write side
    # ------------------------------------------------------------------ #

    def insert_job(self, job: Job) -> bool:
        """Insert a new job. Returns False if the idempotency key exists."""
        params = (
            job.id, job.idempotency_key, job.payload_hash, job.job_type,
            json.dumps(job.payload, ensure_ascii=False), job.max_attempts,
            job.state.value, job.error_class.value, job.error_message,
            None if job.result is None else json.dumps(job.result, ensure_ascii=False),
            job.attempt, job.created_at, job.updated_at, job.enqueued_at,
            job.not_before, job.deadline_queue, job.started_at,
            job.finished_at, json.dumps(job.attempt_history, ensure_ascii=False),
        )
        placeholders = ",".join("?" for _ in _COLUMNS)
        with self._lock:
            try:
                self._conn.execute(
                    f"INSERT INTO jobs ({','.join(_COLUMNS)}) "
                    f"VALUES ({placeholders})",
                    params,
                )
            except sqlite3.IntegrityError:
                return False
        return True

    def transition(
        self,
        job_id: str,
        expected: Iterable[JobState],
        *,
        state: JobState,
        error_class: ErrorClass | None = None,
        error_message: str | None = None,
        result: Any = ...,
        attempt: int | None = None,
        not_before: float | None = None,
        enqueued_at: float | None = None,
        started_at: float | None = None,
        finished_at: float | None = None,
        append_history: dict[str, Any] | None = None,
    ) -> bool:
        """Atomic compare-and-set transition.

        Succeeds only when the current state is in ``expected``. Fields
        passed as None are left untouched (except ``error_message`` /
        ``enqueued_at`` / ``started_at`` / ``finished_at`` which can be
        explicitly cleared — callers never need that today). ``result``
        defaults to an ellipsis sentinel meaning "do not change".
        Returns True if a row was updated.
        """
        expected_states = [s.value for s in expected]
        if not expected_states:
            return False
        now = time.time()
        sets: list[str] = ["state = ?", "updated_at = ?"]
        params: list[Any] = [state.value, now]

        if error_class is not None:
            sets.append("error_class = ?")
            params.append(error_class.value)
        if error_message is not None:
            sets.append("error_message = ?")
            params.append(error_message)
        if result is not ...:
            sets.append("result = ?")
            params.append(
                None if result is None else json.dumps(result, ensure_ascii=False)
            )
        if attempt is not None:
            sets.append("attempt = ?")
            params.append(attempt)
        if not_before is not None:
            sets.append("not_before = ?")
            params.append(not_before)
        if enqueued_at is not None:
            sets.append("enqueued_at = ?")
            params.append(enqueued_at)
        if started_at is not None:
            sets.append("started_at = ?")
            params.append(started_at)
        if finished_at is not None:
            sets.append("finished_at = ?")
            params.append(finished_at)

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if append_history is not None:
                    row = self._conn.execute(
                        "SELECT attempt_history FROM jobs WHERE id = ?",
                        (job_id,),
                    ).fetchone()
                    if row is None:
                        self._conn.execute("ROLLBACK")
                        return False
                    history = json.loads(row["attempt_history"])
                    history.append(append_history)
                    sets.append("attempt_history = ?")
                    params.append(json.dumps(history, ensure_ascii=False))

                params.append(job_id)
                placeholders = ",".join("?" for _ in expected_states)
                # Final parameter order: SET values, id, expected states.
                sql_params = params[: len(sets)] + [job_id] + expected_states
                sql = (
                    f"UPDATE jobs SET {', '.join(sets)} "
                    f"WHERE id = ? AND state IN ({placeholders})"
                )
                cursor = self._conn.execute(sql, sql_params)
                self._conn.execute("COMMIT")
                return cursor.rowcount == 1
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def expire_queue_timeouts(self, now: float) -> list[Job]:
        """Move QUEUED jobs past their queue deadline to TIMED_OUT."""
        expired: list[Job] = []
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    "SELECT * FROM jobs "
                    "WHERE state = 'QUEUED' AND deadline_queue IS NOT NULL "
                    "AND deadline_queue <= ?",
                    (now,),
                ).fetchall()
                jobs = [self._row_to_job(r) for r in rows]
                for job in jobs:
                    self._conn.execute(
                        "UPDATE jobs SET state = ?, error_class = ?, "
                        "error_message = ?, updated_at = ?, finished_at = ? "
                        "WHERE id = ? AND state = 'QUEUED'",
                        (
                            JobState.TIMED_OUT.value,
                            ErrorClass.QUEUE_TIMEOUT.value,
                            "job exceeded maximum queue wait time",
                            now, now, job.id,
                        ),
                    )
                self._conn.execute("COMMIT")
                expired = jobs
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return expired

    def recover_on_startup(
        self,
        now: float,
        *,
        queue_timeout: float,
        retry_delay_for: Any,
    ) -> list[Job]:
        """Reconcile durable state with a fresh process.

        * stale RUNNING rows -> FAILED(INTERRUPTED) after a hard crash: the
          attempt died with the process and is never re-executed (at-least-
          once guard); after an *orderly* runner shutdown they become
          CANCELLED (the stop deliberately aborted the attempt);
        * QUEUED rows have their scheduling clock (``enqueued_at`` /
          ``not_before`` / ``deadline_queue``) rebased onto the new
          process's monotonic clock: they resume waiting without being
          instantly expired by the dead process's old clock;
        * AWAITING_RETRY rows keep their remaining attempts but get a fresh
          ``not_before`` via ``retry_delay_for(next_attempt)``.
        Nothing is silently lost; the dispatcher picks every row returned.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                orderly = self.is_orderly_shutdown()
                if orderly:
                    stale_state, stale_class, stale_msg = (
                        JobState.CANCELLED.value,
                        ErrorClass.CANCELLED.value,
                        "attempt aborted by orderly service shutdown",
                    )
                else:
                    stale_state, stale_class, stale_msg = (
                        JobState.FAILED.value,
                        ErrorClass.INTERRUPTED.value,
                        "process restarted while attempt was running",
                    )
                interrupted_rows = self._conn.execute(
                    "SELECT * FROM jobs WHERE state = 'RUNNING'"
                ).fetchall()
                for row in interrupted_rows:
                    self._conn.execute(
                        "UPDATE jobs SET state = ?, error_class = ?, "
                        "error_message = ?, updated_at = ?, finished_at = ? "
                        "WHERE id = ?",
                        (stale_state, stale_class, stale_msg,
                         now, now, row["id"]),
                    )
                self.clear_shutdown_marker()

                queued_rows = self._conn.execute(
                    "SELECT * FROM jobs WHERE state = 'QUEUED'"
                ).fetchall()
                for row in queued_rows:
                    self._conn.execute(
                        "UPDATE jobs SET enqueued_at = ?, not_before = ?, "
                        "deadline_queue = ?, updated_at = ? WHERE id = ?",
                        (now, now, now + queue_timeout, now, row["id"]),
                    )

                retry_rows = self._conn.execute(
                    "SELECT * FROM jobs WHERE state = 'AWAITING_RETRY'"
                ).fetchall()
                for row in retry_rows:
                    next_attempt = row["attempt"] + 1
                    not_before = now + float(retry_delay_for(next_attempt))
                    self._conn.execute(
                        "UPDATE jobs SET not_before = ?, updated_at = ? "
                        "WHERE id = ?",
                        (not_before, now, row["id"]),
                    )

                due_rows = self._conn.execute(
                    "SELECT * FROM jobs "
                    "WHERE state IN ('QUEUED', 'AWAITING_RETRY') "
                    "ORDER BY not_before ASC, created_at ASC, id ASC"
                ).fetchall()
                self._conn.execute("COMMIT")
                recovered = [self._row_to_job(r) for r in due_rows]
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return recovered
