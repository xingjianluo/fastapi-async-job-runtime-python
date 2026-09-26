"""Runtime configuration.

All knobs are overridable via environment variables (prefix ``JOB_RUNTIME_``)
and the :class:`RuntimeConfig` constructor. Nothing here requires an
external service.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


@dataclass(frozen=True)
class RuntimeConfig:
    # Concurrency: at most this many jobs execute at the same time.
    max_concurrency: int = 4
    # Hard limit of jobs waiting in the queue; beyond this submissions block.
    max_queue_size: int = 1000
    # Default per-attempt execution timeout (seconds).
    attempt_timeout: float = 30.0
    # Default maximum number of attempts per job (1 = no retries).
    default_max_attempts: int = 3
    # Default maximum time a job may spend QUEUED before TIMED_OUT.
    queue_timeout: float = 600.0
    # Retry backoff: delay(n) = base_delay * backoff_factor ** (n - 1).
    backoff_base_delay: float = 0.2
    backoff_factor: float = 2.0
    backoff_max_delay: float = 30.0
    # Worker poll tick when the queue is empty / slots busy.
    poll_interval: float = 0.05
    # SQLite database file (":memory:" for ephemeral tests).
    db_path: str = "job_runtime.db"

    @classmethod
    def from_env(cls) -> "RuntimeConfig":
        return cls(
            max_concurrency=_env_int("JOB_RUNTIME_MAX_CONCURRENCY", 4),
            max_queue_size=_env_int("JOB_RUNTIME_MAX_QUEUE_SIZE", 1000),
            attempt_timeout=_env_float("JOB_RUNTIME_ATTEMPT_TIMEOUT", 30.0),
            default_max_attempts=_env_int("JOB_RUNTIME_DEFAULT_MAX_ATTEMPTS", 3),
            queue_timeout=_env_float("JOB_RUNTIME_QUEUE_TIMEOUT", 600.0),
            backoff_base_delay=_env_float("JOB_RUNTIME_BACKOFF_BASE", 0.2),
            backoff_factor=_env_float("JOB_RUNTIME_BACKOFF_FACTOR", 2.0),
            backoff_max_delay=_env_float("JOB_RUNTIME_BACKOFF_MAX", 30.0),
            poll_interval=_env_float("JOB_RUNTIME_POLL_INTERVAL", 0.05),
            db_path=os.environ.get("JOB_RUNTIME_DB", "job_runtime.db"),
        )
