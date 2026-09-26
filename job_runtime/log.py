"""Structured-ish logging helpers.

Every transition line contains the job id, the state change and, when
relevant, the error classification so test output is self-explanatory.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("job_runtime")

_DEFAULT_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    """Attach a stream handler once (idempotent across test sessions)."""
    if not logging.getLogger("job_runtime").handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT))
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False


def log_job_event(
    job_id: str,
    event: str,
    *,
    from_state: str | None = None,
    to_state: str | None = None,
    error_class: str | None = None,
    attempt: int | None = None,
    detail: str | None = None,
) -> None:
    parts = [f"job={job_id}", f"event={event}"]
    if from_state is not None or to_state is not None:
        parts.append(f"state={from_state or '-'}->{to_state or '-'}")
    if attempt is not None:
        parts.append(f"attempt={attempt}")
    if error_class is not None:
        parts.append(f"error_class={error_class}")
    if detail:
        parts.append(detail)
    logger.info(" | ".join(parts))
