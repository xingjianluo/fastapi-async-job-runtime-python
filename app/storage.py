"""作业持久化：SQLite 单文件存储。

状态流转全部通过条件 UPDATE（CAS）落库，保证终态不可逆、不重复执行、
服务重启后终态结果仍可查询。所有写操作开启外键与 WAL，单进程内
由 JobManager 串行化访问。

调度相关：
- jobs.priority 列记录提交优先级（1 低 / 2 普通 / 3 高），老库通过
  ALTER TABLE 迁移并回填默认值。
- claim_next 在单个 IMMEDIATE 事务内完成“读运行计数 -> 按有效优先级
  挑选 -> CAS 领取”，保证类型额度不会被并发领取击穿。
"""

from __future__ import annotations

import datetime as _dt
import json
import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from app.errors import ErrorCode
from app.models import (
    AttemptRecord,
    FailureKind,
    JobRecord,
    JobResult,
    JobState,
    Priority,
)

_TERMINAL_VALUES = (
    JobState.SUCCEEDED.value,
    JobState.FAILED.value,
    JobState.CANCELLED.value,
    JobState.TIMED_OUT.value,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    idempotency_key TEXT,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    priority INTEGER NOT NULL DEFAULT 2,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    not_before TEXT,
    queued_deadline TEXT,
    run_deadline TEXT,
    started_at TEXT,
    finished_at TEXT,
    output TEXT,
    error_code TEXT,
    error_message TEXT,
    failure_kind TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idem_key
    ON jobs(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_claim
    ON jobs(state, not_before);
CREATE TABLE IF NOT EXISTS job_attempts (
    job_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    state_after TEXT NOT NULL,
    result TEXT,
    PRIMARY KEY (job_id, attempt)
);
"""


def _parse_iso(value: str) -> _dt.datetime:
    return _dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
        tzinfo=_dt.UTC
    )


def effective_priority(
    job: JobRecord,
    now: _dt.datetime,
    aging_threshold: float | None,
    aging_max_boost: int,
) -> tuple[int, int]:
    """计算有效优先级与等待提升档数（确定且可解释的规则）。

    规则：作业自 not_before（可执行时刻）起，每完整等待 aging_threshold
    秒提升 1 档，最多提升 aging_max_boost 档，且不超过 Priority.MAX。
    aging_threshold 为空或 <=0 时关闭等待提升。
    返回 (有效优先级, 提升档数)。
    """
    boost = 0
    if aging_threshold and aging_threshold > 0 and job.not_before:
        waited = (now - _parse_iso(job.not_before)).total_seconds()
        if waited > 0:
            boost = min(aging_max_boost, int(waited // aging_threshold))
    effective = min(Priority.MAX, job.priority + boost)
    return effective, boost


@dataclass
class ClaimInfo:
    """一次成功领取的调度留痕，供日志与测试断言。"""

    job: JobRecord
    effective_priority: int
    aging_boost: int
    type_running: int
    type_quota: int | None


class DuplicateIdempotencyKey(Exception):
    """并发下唯一幂等键冲突，由上层重新查询判定。"""


def _row_to_job(cur: sqlite3.Cursor, row: sqlite3.Row) -> JobRecord:
    attempt_rows = cur.execute(
        "SELECT attempt, started_at, finished_at, state_after, result "
        "FROM job_attempts WHERE job_id = ? ORDER BY attempt",
        (row["id"],),
    ).fetchall()
    attempts: list[AttemptRecord] = []
    for a in attempt_rows:
        result = None
        if a["result"]:
            rdata = json.loads(a["result"])
            result = JobResult(
                ok=rdata["ok"],
                output=rdata.get("output"),
                failure_kind=(
                    FailureKind(rdata["failure_kind"])
                    if rdata.get("failure_kind")
                    else None
                ),
                error_message=rdata.get("error_message"),
                error_type=rdata.get("error_type"),
            )
        attempts.append(
            AttemptRecord(
                attempt=a["attempt"],
                started_at=a["started_at"],
                finished_at=a["finished_at"],
                state_after=a["state_after"],
                result=result,
            )
        )
    return JobRecord(
        id=row["id"],
        job_type=row["job_type"],
        payload=json.loads(row["payload"]),
        idempotency_key=row["idempotency_key"],
        state=JobState(row["state"]),
        attempt=row["attempt"],
        max_attempts=row["max_attempts"],
        priority=row["priority"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        not_before=row["not_before"],
        queued_deadline=row["queued_deadline"],
        run_deadline=row["run_deadline"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        output=(json.loads(row["output"]) if row["output"] is not None else None),
        error_code=row["error_code"],
        error_message=row["error_message"],
        failure_kind=row["failure_kind"],
        attempts=attempts,
    )


class JobStorage(ABC):
    """存储抽象，便于测试时替换。"""

    @abstractmethod
    def init_schema(self) -> None: ...

    @abstractmethod
    def insert_job(self, job: JobRecord) -> None: ...

    @abstractmethod
    def get_job(self, job_id: str) -> JobRecord | None: ...

    @abstractmethod
    def get_by_idempotency_key(self, key: str) -> JobRecord | None: ...

    @abstractmethod
    def count_waiting(self) -> int:
        """统计等待中的作业数（PENDING/RETRY_WAIT，不含 RUNNING）。"""

    @abstractmethod
    def claim_next(
        self,
        now_iso: str,
        run_deadline_for: Callable[[JobRecord], str],
        type_quotas: dict[str, int],
        aging_threshold: float | None,
        aging_max_boost: int,
    ) -> ClaimInfo | None:
        """原子领取一个到期可执行且所属类型额度未满的作业并置为 RUNNING。

        挑选规则：有效优先级（优先级 + 等待提升）高者优先，同档按
        created_at 先到先得；类型未配置额度时仅受全局并发约束。
        run_deadline_for 在事务内对被选中作业计算其执行截止时间。
        """

    @abstractmethod
    def peek_next_due(self, now_iso: str) -> JobRecord | None:
        """只读探测最早到期的等待作业（不加锁、不改变状态）。"""

    @abstractmethod
    def count_running_by_type(self) -> dict[str, int]:
        """各类型当前 RUNNING 数（用于额度判定与排队原因解释）。"""

    @abstractmethod
    def list_due_waiting(self, now_iso: str) -> list[JobRecord]:
        """当前到期可执行、仍在等待的作业（用于排队原因解释）。"""

    @abstractmethod
    def compare_and_set_state(
        self,
        job_id: str,
        expected: set[JobState],
        target: JobState,
        **fields: Any,
    ) -> bool:
        """仅当当前状态属于 expected 时更新为 target，返回是否成功。"""

    @abstractmethod
    def requeue_running_jobs(self, now_iso: str) -> int:
        """重启恢复：把遗留 RUNNING 的作业置回 PENDING。"""

    @abstractmethod
    def timeout_queued_jobs(self, now_iso: str) -> list[str]:
        """把排队超时的 PENDING/RETRY_WAIT 作业置为 TIMED_OUT。"""

    @abstractmethod
    def append_attempt(self, job_id: JobRecord | str, record: AttemptRecord) -> None: ...

    @abstractmethod
    def list_jobs(self, limit: int = 100) -> list[JobRecord]: ...

    @abstractmethod
    def close(self) -> None: ...


# 允许通过 CAS 更新的列白名单，避免 SQL 拼接注入。
_ALLOWED_FIELDS = frozenset(
    {
        "attempt",
        "updated_at",
        "not_before",
        "queued_deadline",
        "run_deadline",
        "started_at",
        "finished_at",
        "output",
        "error_code",
        "error_message",
        "failure_kind",
        "payload",
    }
)


class SqliteJobStorage(JobStorage):
    """SQLite 实现。"""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=5000")
            self._conn = conn
        return self._conn

    def init_schema(self) -> None:
        conn = self._connect()
        conn.executescript(_SCHEMA)
        # 老库迁移：补 priority 列并回填默认优先级。
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(jobs)")}
        if "priority" not in cols:
            conn.execute(
                "ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 2"
            )
        conn.commit()

    def insert_job(self, job: JobRecord) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO jobs (id, job_type, payload, idempotency_key, state, "
                "attempt, max_attempts, priority, created_at, updated_at, not_before, "
                "queued_deadline, run_deadline, started_at, finished_at, output, "
                "error_code, error_message, failure_kind) VALUES (?,?,?,?,?,?,?,?,?,"
                "?,?,?,?,?,?,?,?,?,?)",
                (
                    job.id,
                    job.job_type,
                    json.dumps(job.payload, ensure_ascii=False),
                    job.idempotency_key,
                    job.state.value,
                    job.attempt,
                    job.max_attempts,
                    job.priority,
                    job.created_at,
                    job.updated_at,
                    job.not_before,
                    job.queued_deadline,
                    job.run_deadline,
                    job.started_at,
                    job.finished_at,
                    json.dumps(job.output, ensure_ascii=False)
                    if job.output is not None
                    else None,
                    job.error_code,
                    job.error_message,
                    job.failure_kind,
                ),
            )
            conn.commit()
        except sqlite3.IntegrityError as exc:
            conn.rollback()
            raise DuplicateIdempotencyKey(str(exc)) from exc

    def _fetch(self, cur: sqlite3.Cursor, job_id: str) -> JobRecord | None:
        row = cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return _row_to_job(cur, row) if row is not None else None

    def get_job(self, job_id: str) -> JobRecord | None:
        cur = self._connect().execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        )
        row = cur.fetchone()
        return _row_to_job(cur, row) if row is not None else None

    def get_by_idempotency_key(self, key: str) -> JobRecord | None:
        cur = self._connect().execute(
            "SELECT * FROM jobs WHERE idempotency_key = ?", (key,)
        )
        row = cur.fetchone()
        return _row_to_job(cur, row) if row is not None else None

    def count_waiting(self) -> int:
        """等待中（PENDING/RETRY_WAIT，不含 RUNNING）的作业数。"""
        row = self._connect().execute(
            "SELECT COUNT(*) AS c FROM jobs WHERE state IN (?,?)",
            (JobState.PENDING.value, JobState.RETRY_WAIT.value),
        ).fetchone()
        return int(row["c"])

    def claim_next(
        self,
        now_iso: str,
        run_deadline_for: Callable[[JobRecord], str],
        type_quotas: dict[str, int],
        aging_threshold: float | None,
        aging_max_boost: int,
    ) -> ClaimInfo | None:
        """原子领取：在单个 IMMEDIATE 事务内完成额度判定与状态翻转。

        判定时间 now_iso 必须与本轮 timeout_queued_jobs 使用的为同一个值：
        timeout 取 queued_deadline <= now，claim 取 queued_deadline > now，
        二者互斥且穷尽，作业要么按排队超时进终态、要么被领取，不会丢失。
        挑选次序：有效优先级降序，同档按 created_at 升序（先提交先执行）；
        所属类型额度已满的作业本轮跳过，不影响其它类型被领取。
        """
        conn = self._connect()
        now = _parse_iso(now_iso)
        conn.execute("BEGIN IMMEDIATE")
        try:
            running_rows = conn.execute(
                "SELECT job_type, COUNT(*) AS c FROM jobs WHERE state = ? "
                "GROUP BY job_type",
                (JobState.RUNNING.value,),
            ).fetchall()
            running_by_type = {r["job_type"]: int(r["c"]) for r in running_rows}

            waiting_rows = conn.execute(
                "SELECT * FROM jobs WHERE state IN (?, ?) AND not_before <= ? "
                "AND queued_deadline > ?",
                (JobState.PENDING.value, JobState.RETRY_WAIT.value, now_iso, now_iso),
            ).fetchall()

            candidates: list[tuple[int, str, JobRecord, int]] = []
            for row in waiting_rows:
                job = _row_to_job(conn, row)
                eff, boost = effective_priority(
                    job, now, aging_threshold, aging_max_boost
                )
                candidates.append((-eff, job.created_at, job, boost))
            # 有效优先级高者优先；同档先提交先执行（created_at 升序）。
            candidates.sort(key=lambda item: (item[0], item[1]))

            chosen: ClaimInfo | None = None
            for neg_eff, _created, job, boost in candidates:
                quota = type_quotas.get(job.job_type)
                type_running = running_by_type.get(job.job_type, 0)
                if quota is not None and type_running >= quota:
                    continue  # 类型额度已满，本轮跳过该类型
                cur = conn.execute(
                    "UPDATE jobs SET state = ?, run_deadline = ?, "
                    "started_at = COALESCE(started_at, ?), updated_at = ? "
                    "WHERE id = ? AND state IN (?, ?) AND queued_deadline > ?",
                    (
                        JobState.RUNNING.value,
                        run_deadline_for(job),
                        now_iso,
                        now_iso,
                        job.id,
                        JobState.PENDING.value,
                        JobState.RETRY_WAIT.value,
                        now_iso,
                    ),
                )
                if cur.rowcount != 1:
                    continue  # 被并发收口（如排队超时），试下一个候选
                running_by_type[job.job_type] = type_running + 1
                chosen = ClaimInfo(
                    job=self._fetch(conn, job.id),
                    effective_priority=-neg_eff,
                    aging_boost=boost,
                    type_running=type_running,
                    type_quota=quota,
                )
                break
            conn.commit()
            return chosen
        except Exception:
            conn.rollback()
            raise

    def count_running_by_type(self) -> dict[str, int]:
        rows = self._connect().execute(
            "SELECT job_type, COUNT(*) AS c FROM jobs WHERE state = ? "
            "GROUP BY job_type",
            (JobState.RUNNING.value,),
        ).fetchall()
        return {r["job_type"]: int(r["c"]) for r in rows}

    def list_due_waiting(self, now_iso: str) -> list[JobRecord]:
        cur = self._connect().execute(
            "SELECT * FROM jobs WHERE state IN (?, ?) AND not_before <= ? "
            "AND queued_deadline > ? ORDER BY created_at ASC",
            (JobState.PENDING.value, JobState.RETRY_WAIT.value, now_iso, now_iso),
        )
        return [_row_to_job(cur, row) for row in cur.fetchall()]

    def peek_next_due(self, now_iso: str) -> JobRecord | None:
        cur = self._connect().execute(
            "SELECT * FROM jobs WHERE state IN (?, ?) AND (not_before <= ?) "
            "AND queued_deadline > ? ORDER BY created_at ASC LIMIT 1",
            (JobState.PENDING.value, JobState.RETRY_WAIT.value, now_iso, now_iso),
        )
        row = cur.fetchone()
        return _row_to_job(cur, row) if row is not None else None

    def compare_and_set_state(
        self,
        job_id: str,
        expected: set[JobState],
        target: JobState,
        **fields: Any,
    ) -> bool:
        bad = set(fields) - _ALLOWED_FIELDS
        if bad:
            raise ValueError(f"非法更新列: {sorted(bad)}")
        set_cols = ["state = ?"]
        params: list[Any] = [target.value]
        for key, value in fields.items():
            if key in ("payload", "output") and value is not None:
                value = json.dumps(value, ensure_ascii=False)
            set_cols.append(f"{key} = ?")
            params.append(value)
        expected_values = tuple(s.value for s in expected)
        placeholders = ",".join("?" for _ in expected_values)
        params.extend([job_id, *expected_values])
        conn = self._connect()
        cur = conn.execute(
            f"UPDATE jobs SET {', '.join(set_cols)} "
            f"WHERE id = ? AND state IN ({placeholders})",
            params,
        )
        conn.commit()
        return cur.rowcount == 1

    def requeue_running_jobs(self, now_iso: str) -> int:
        conn = self._connect()
        cur = conn.execute(
            "UPDATE jobs SET state = ?, not_before = ?, run_deadline = NULL, "
            "updated_at = ? WHERE state = ?",
            (JobState.PENDING.value, now_iso, now_iso, JobState.RUNNING.value),
        )
        conn.commit()
        return cur.rowcount

    def timeout_queued_jobs(self, now_iso: str) -> list[str]:
        conn = self._connect()
        cur = conn.execute(
            "UPDATE jobs SET state = ?, finished_at = ?, updated_at = ?, "
            "error_code = ?, error_message = ?, failure_kind = ? "
            "WHERE state IN (?, ?) AND queued_deadline <= ? RETURNING id",
            (
                JobState.TIMED_OUT.value,
                now_iso,
                now_iso,
                ErrorCode.QUEUE_TIMEOUT.value,
                "排队等待超过 queue_timeout，按确定规则置为 TIMED_OUT",
                FailureKind.TIMEOUT.value,
                JobState.PENDING.value,
                JobState.RETRY_WAIT.value,
                now_iso,
            ),
        )
        ids = [r["id"] for r in cur.fetchall()]
        conn.commit()
        return ids

    def append_attempt(self, job_id: JobRecord | str, record: AttemptRecord) -> None:
        jid = job_id.id if isinstance(job_id, JobRecord) else job_id
        result_json = (
            json.dumps(record.result.to_dict(), ensure_ascii=False)
            if record.result is not None
            else None
        )
        conn = self._connect()
        conn.execute(
            "INSERT OR REPLACE INTO job_attempts (job_id, attempt, started_at, "
            "finished_at, state_after, result) VALUES (?,?,?,?,?,?)",
            (
                jid,
                record.attempt,
                record.started_at,
                record.finished_at,
                record.state_after,
                result_json,
            ),
        )
        conn.commit()

    def list_jobs(self, limit: int = 100) -> list[JobRecord]:
        cur = self._connect().execute(
            "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (max(1, min(limit, 500)),)
        )
        rows = cur.fetchall()
        return [_row_to_job(cur, row) for row in rows]

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
