"""Handler registry and the failure contract used by job implementations.

A handler is an ``async`` callable receiving the job id and payload::

    async def my_handler(job_id: str, payload: dict) -> Any: ...

Failures are communicated by raising :class:`JobExecutionError`. Retryable
errors trigger bounded backoff retries; non-retryable errors fail the job
immediately. Any other exception is treated as NON_RETRYABLE.

Built-in handlers (all local, no external services):

* ``succeed``           — returns ``{"echo": ...}`` after an optional delay
* ``fail_retryable``    — always fails with a retryable classification
* ``fail_permanent``    — always fails with a non-retryable classification
* ``flaky``             — fails the first ``fail_times`` attempts, then succeeds
* ``slow``              — sleeps ``seconds``; used to exercise attempt timeouts
* ``boom``              — raises an unexpected (unclassified) exception
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict

from .models import ErrorClass

Handler = Callable[[str, dict[str, Any]], Awaitable[Any]]


class JobExecutionError(Exception):
    """Raised by handlers to declare an explicit, classifiable failure."""

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.message = message
        self.retryable = retryable

    @property
    def error_class(self) -> ErrorClass:
        return ErrorClass.RETRYABLE if self.retryable else ErrorClass.NON_RETRYABLE


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: Dict[str, Handler] = {}

    def register(self, job_type: str, handler: Handler) -> None:
        if not job_type:
            raise ValueError("job_type must be non-empty")
        self._handlers[job_type] = handler

    def get(self, job_type: str) -> Handler | None:
        return self._handlers.get(job_type)

    def contains(self, job_type: str) -> bool:
        return job_type in self._handlers


# --------------------------------------------------------------------------- #
# Built-in handlers
# --------------------------------------------------------------------------- #

# Shared per-flaky-key attempt counter (process local; sufficient for local
# runs and tests — durable cross-attempt state lives in job.attempt anyway).
_FLAKY_ATTEMPTS: Dict[str, int] = {}


def reset_flaky_counters() -> None:
    _FLAKY_ATTEMPTS.clear()


async def succeed_handler(job_id: str, payload: dict[str, Any]) -> Any:
    delay = float(payload.get("delay", 0))
    if delay > 0:
        await asyncio.sleep(delay)
    return {"echo": payload, "job_id": job_id}


async def fail_retryable_handler(job_id: str, payload: dict[str, Any]) -> Any:
    raise JobExecutionError("transient failure, retry me", retryable=True)


async def fail_permanent_handler(job_id: str, payload: dict[str, Any]) -> Any:
    raise JobExecutionError("permanent business failure", retryable=False)


async def flaky_handler(job_id: str, payload: dict[str, Any]) -> Any:
    key = str(payload.get("key", job_id))
    fail_times = int(payload.get("fail_times", 2))
    seen = _FLAKY_ATTEMPTS.get(key, 0) + 1
    _FLAKY_ATTEMPTS[key] = seen
    if seen <= fail_times:
        raise JobExecutionError(
            f"flaky failure {seen}/{fail_times}", retryable=True
        )
    return {"succeeded_on_attempt": seen, "key": key}


async def slow_handler(job_id: str, payload: dict[str, Any]) -> Any:
    seconds = float(payload.get("seconds", 10))
    await asyncio.sleep(seconds)
    return {"slept": seconds}


async def boom_handler(job_id: str, payload: dict[str, Any]) -> Any:
    raise RuntimeError("unexpected unclassified handler error")


def register_builtin_handlers(registry: HandlerRegistry) -> None:
    """Register demo handlers used for local runs and tests."""
    registry.register("succeed", succeed_handler)
    registry.register("fail_retryable", fail_retryable_handler)
    registry.register("fail_permanent", fail_permanent_handler)
    registry.register("flaky", flaky_handler)
    registry.register("slow", slow_handler)
    registry.register("boom", boom_handler)
