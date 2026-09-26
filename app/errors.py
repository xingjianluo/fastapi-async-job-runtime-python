"""可区分的业务错误码与异常。"""

from __future__ import annotations

from enum import Enum
from typing import Any


class ErrorCode(str, Enum):
    """错误原因分类，HTTP 响应与日志中都会带上，便于区分。"""

    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    JOB_NOT_FOUND = "JOB_NOT_FOUND"
    ALREADY_TERMINAL = "ALREADY_TERMINAL"
    JOB_ALREADY_CANCELLED = "JOB_ALREADY_CANCELLED"
    QUEUE_FULL = "QUEUE_FULL"
    UNKNOWN_JOB_TYPE = "UNKNOWN_JOB_TYPE"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    RETRYABLE_FAILURE = "RETRYABLE_FAILURE"
    NON_RETRYABLE_FAILURE = "NON_RETRYABLE_FAILURE"
    ATTEMPT_TIMEOUT = "ATTEMPT_TIMEOUT"
    QUEUE_TIMEOUT = "QUEUE_TIMEOUT"


class JobError(Exception):
    """业务异常基类：携带稳定错误码与 HTTP 状态码。"""

    error_code: ErrorCode = ErrorCode.VALIDATION_ERROR
    http_status: int = 400

    def __init__(
        self,
        message: str,
        *,
        error_code: ErrorCode | None = None,
        http_status: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        if error_code is not None:
            self.error_code = error_code
        if http_status is not None:
            self.http_status = http_status
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.error_code.value,
                "message": self.message,
                "details": self.details,
            }
        }


class IdempotencyConflict(JobError):
    def __init__(self, message: str, *, existing_job_id: str) -> None:
        super().__init__(
            message,
            error_code=ErrorCode.IDEMPOTENCY_CONFLICT,
            http_status=409,
            details={"existing_job_id": existing_job_id},
        )


class JobNotFound(JobError):
    def __init__(self, job_id: str) -> None:
        super().__init__(
            f"作业不存在: {job_id}",
            error_code=ErrorCode.JOB_NOT_FOUND,
            http_status=404,
            details={"job_id": job_id},
        )


class AlreadyTerminal(JobError):
    def __init__(self, job_id: str, state: str, operation: str) -> None:
        super().__init__(
            f"作业已处于终态 {state}，不能执行 {operation}",
            error_code=ErrorCode.ALREADY_TERMINAL,
            http_status=409,
            details={"job_id": job_id, "state": state, "operation": operation},
        )


class QueueFull(JobError):
    def __init__(self, max_queue_size: int) -> None:
        super().__init__(
            f"等待队列已满（容量 {max_queue_size}），请稍后重试",
            error_code=ErrorCode.QUEUE_FULL,
            http_status=429,
            details={"max_queue_size": max_queue_size},
        )


class UnknownJobType(JobError):
    def __init__(self, job_type: str, known: list[str]) -> None:
        super().__init__(
            f"未知作业类型: {job_type}",
            error_code=ErrorCode.UNKNOWN_JOB_TYPE,
            http_status=400,
            details={"job_type": job_type, "known_types": known},
        )
