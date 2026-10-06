"""作业领域模型：状态机、作业记录、提交载荷。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar

from pydantic import BaseModel, Field, field_validator


class JobState(str, Enum):
    """作业状态机：

    PENDING -> RUNNING -> SUCCEEDED
                       \\-> RETRY_WAIT -> PENDING（有限退避重试）
                       \\-> FAILED / TIMED_OUT
    PENDING/RUNNING/RETRY_WAIT -> CANCELLED（仅非终态可取消）

    SUCCEEDED / FAILED / CANCELLED / TIMED_OUT 为终态，不可逆。
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"


TERMINAL_STATES = frozenset(
    {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED, JobState.TIMED_OUT}
)


class FailureKind(str, Enum):
    """失败分类：决定立即失败还是可退避重试。"""

    RETRYABLE = "RETRYABLE"
    NON_RETRYABLE = "NON_RETRYABLE"
    TIMEOUT = "TIMEOUT"


class Priority:
    """优先级档位（数值越大越优先）。老请求缺省为 NORMAL，行为不变。"""

    LOW: ClassVar[int] = 1
    NORMAL: ClassVar[int] = 2
    HIGH: ClassVar[int] = 3

    MIN: ClassVar[int] = LOW
    MAX: ClassVar[int] = HIGH
    DEFAULT: ClassVar[int] = NORMAL

    NAMES: ClassVar[dict[int, str]] = {LOW: "LOW", NORMAL: "NORMAL", HIGH: "HIGH"}


class QueueReason(str, Enum):
    """作业仍在等待的调度原因（查询时现算，不落库）。

    - QUEUED: 等待空闲的全局执行名额（worker 都被占满）
    - TYPE_QUOTA_FULL: 所属类型的并发额度已占满
    - PRIORITY_WAIT: 有更高（有效）优先级的到期作业排在前面
    - RETRY_BACKOFF: 失败重试退避中，尚未到下次可执行时间
    """

    QUEUED = "QUEUED"
    TYPE_QUOTA_FULL = "TYPE_QUOTA_FULL"
    PRIORITY_WAIT = "PRIORITY_WAIT"
    RETRY_BACKOFF = "RETRY_BACKOFF"


@dataclass
class JobResult:
    """单次执行尝试的结果。"""

    ok: bool
    output: Any = None
    failure_kind: FailureKind | None = None
    error_message: str | None = None
    error_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "output": self.output,
            "failure_kind": self.failure_kind.value if self.failure_kind else None,
            "error_message": self.error_message,
            "error_type": self.error_type,
        }


@dataclass
class AttemptRecord:
    """一次执行尝试的留痕，用于查询与日志。"""

    attempt: int
    started_at: str
    finished_at: str
    state_after: str
    result: JobResult | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempt": self.attempt,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "state_after": self.state_after,
            "result": self.result.to_dict() if self.result else None,
        }


@dataclass
class JobRecord:
    """持久化的作业记录，也是对外查询的数据源。"""

    id: str
    job_type: str
    payload: dict[str, Any]
    idempotency_key: str | None
    state: JobState
    attempt: int
    max_attempts: int
    created_at: str
    updated_at: str
    priority: int = Priority.DEFAULT
    not_before: str | None = None
    queued_deadline: str | None = None
    run_deadline: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    output: Any = None
    error_code: str | None = None
    error_message: str | None = None
    failure_kind: str | None = None
    attempts: list[AttemptRecord] = field(default_factory=list)
    # 查询时现算的调度等待原因（仅 PENDING/RETRY_WAIT 时有值），不落库。
    queue_reason: str | None = None

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "job_type": self.job_type,
            "payload": self.payload,
            "idempotency_key": self.idempotency_key,
            "state": self.state.value,
            "terminal": self.state in TERMINAL_STATES,
            "priority": self.priority,
            "priority_name": Priority.NAMES.get(self.priority, str(self.priority)),
            "queue_reason": self.queue_reason,
            "attempt": self.attempt,
            "max_attempts": self.max_attempts,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "not_before": self.not_before,
            "queued_deadline": self.queued_deadline,
            "run_deadline": self.run_deadline,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "output": self.output,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "failure_kind": self.failure_kind,
            "attempts": [a.to_dict() for a in self.attempts],
        }


class JobSubmitRequest(BaseModel):
    """提交作业的请求体。"""

    job_type: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)
    max_attempts: int | None = Field(default=None, ge=1, le=20)
    run_timeout: float | None = Field(default=None, gt=0, le=3600)
    queue_timeout: float | None = Field(default=None, gt=0, le=3600)
    # 优先级：1=低 2=普通（默认，老请求缺省行为不变）3=高
    priority: int = Field(
        default=Priority.DEFAULT, ge=Priority.MIN, le=Priority.MAX
    )


class JobSubmitResponse(BaseModel):
    id: str
    state: JobState
    deduplicated: bool


class JobCancellationResponse(BaseModel):
    id: str
    state: JobState


class QuotaUpdateRequest(BaseModel):
    """整体替换类型并发额度的请求体。额度为 0 表示暂停该类型新领取。"""

    quotas: dict[str, int] = Field(default_factory=dict)

    @field_validator("quotas")
    @classmethod
    def _validate_quotas(cls, value: dict[str, int]) -> dict[str, int]:
        for job_type, quota in value.items():
            if not job_type:
                raise ValueError("作业类型名不能为空")
            if quota < 0 or quota > 10000:
                raise ValueError(f"类型 {job_type!r} 的额度必须在 0..10000 之间")
        return value


class QuotaConfigResponse(BaseModel):
    quotas: dict[str, int]
    max_concurrency: int
