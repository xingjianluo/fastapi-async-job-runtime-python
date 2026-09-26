"""Domain types for the async job runtime.

This module only contains *type definitions*: enums, dataclasses,
Pydantic API models and the API error hierarchy. It holds no business
logic so it can be imported by any other module without cycles.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class JobState(str, Enum):
    """Explicit job states (see docs/state-machine.md)."""

    QUEUED = "QUEUED"            # accepted, waiting in the bounded queue
    RUNNING = "RUNNING"          # a worker is executing an attempt
    AWAITING_RETRY = "AWAITING_RETRY"  # attempt failed (retryable), backing off
    SUCCEEDED = "SUCCEEDED"      # terminal
    FAILED = "FAILED"            # terminal: non-retryable error / retry exhausted
    CANCELLED = "CANCELLED"      # terminal: cancelled before/during execution
    TIMED_OUT = "TIMED_OUT"      # terminal: queue wait timeout / attempt timeout exhausted


TERMINAL_STATES = frozenset(
    {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED, JobState.TIMED_OUT}
)
ACTIVE_STATES = frozenset(
    {JobState.QUEUED, JobState.RUNNING, JobState.AWAITING_RETRY}
)


class ErrorClass(str, Enum):
    """Coarse, distinguishable classification of a failure."""

    NONE = "NONE"                 # no error (success / not failed yet)
    RETRYABLE = "RETRYABLE"       # transient failure, attempt may be retried
    NON_RETRYABLE = "NON_RETRYABLE"  # permanent business failure
    TIMEOUT = "TIMEOUT"           # execution attempt exceeded per-attempt timeout
    QUEUE_TIMEOUT = "QUEUE_TIMEOUT"  # spent too long waiting in the queue
    CANCELLED = "CANCELLED"       # cancelled by the caller
    INTERRUPTED = "INTERRUPTED"   # process died while the attempt was running
    PAYLOAD_CONFLICT = "PAYLOAD_CONFLICT"


def new_job_id() -> str:
    return uuid.uuid4().hex


def compute_payload_hash(job_type: str, payload: dict[str, Any]) -> str:
    """Deterministic hash of (type, payload) for idempotency conflict detection."""
    canonical = json.dumps(
        {"type": job_type, "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass
class Job:
    """The full, persistable job record."""

    id: str
    idempotency_key: str | None
    payload_hash: str
    job_type: str
    payload: dict[str, Any]
    max_attempts: int
    state: JobState
    error_class: ErrorClass = ErrorClass.NONE
    error_message: str | None = None
    result: Any = None
    attempt: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    enqueued_at: float | None = None
    not_before: float = 0.0          # earliest dequeue time (retry backoff)
    deadline_queue: float | None = None
    started_at: float | None = None
    finished_at: float | None = None
    attempt_history: list[dict[str, Any]] = field(default_factory=list)

    def to_api_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "state": self.state.value,
            "attempt": self.attempt,
            "max_attempts": self.max_attempts,
            "job_type": self.job_type,
            "result": self.result,
            "error_class": self.error_class.value,
            "error_message": self.error_message,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "attempt_history": self.attempt_history,
        }


# --------------------------------------------------------------------------- #
# API wire models
# --------------------------------------------------------------------------- #


class JobSubmitRequest(BaseModel):
    job_type: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)
    max_attempts: int | None = Field(default=None, ge=1, le=100)


class JobView(BaseModel):
    id: str
    state: str
    attempt: int
    max_attempts: int
    job_type: str
    result: Any = None
    error_class: str
    error_message: str | None = None
    created_at: float
    updated_at: float
    started_at: float | None = None
    finished_at: float | None = None
    attempt_history: list[dict[str, Any]] = Field(default_factory=list)


class JobSubmitResponse(BaseModel):
    job: JobView
    reused: bool  # True -> returned an existing job for this idempotency key


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class APIError(Exception):
    """Base class for errors mapped to distinct HTTP responses."""

    status_code: int = 400
    code: str = "bad_request"

    def __init__(self, message: str, *, code: str | None = None,
                 status_code: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code
        if status_code is not None:
            self.status_code = status_code


class IdempotencyConflictError(APIError):
    code = "IDEMPOTENCY_PAYLOAD_CONFLICT"
    status_code = 409


class TerminalStateConflictError(APIError):
    code = "TERMINAL_STATE_CONFLICT"
    status_code = 409


class JobNotFoundError(APIError):
    code = "JOB_NOT_FOUND"
    status_code = 404


class UnknownJobTypeError(APIError):
    code = "UNKNOWN_JOB_TYPE"
    status_code = 422
