"""Application service: submit / get / cancel plus runtime lifecycle.

This is the single facade used by the HTTP layer and by tests; it wires the
store, handler registry and runner together and enforces the idempotency,
bounded-queue and terminal-state rules described in docs/state-machine.md.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from .config import RuntimeConfig
from .handlers import HandlerRegistry, register_builtin_handlers
from .log import configure_logging, log_job_event
from .models import (
    ErrorClass,
    IdempotencyConflictError,
    Job,
    JobNotFoundError,
    JobState,
    TerminalStateConflictError,
    UnknownJobTypeError,
    compute_payload_hash,
    new_job_id,
    TERMINAL_STATES,
)
from .runner import JobRunner
from .storage import JobStore


class QueueFullError(Exception):
    """Raised internally when the bounded queue cannot accept a new job."""


class JobService:
    def __init__(
        self,
        config: RuntimeConfig | None = None,
        *,
        store: JobStore | None = None,
        registry: HandlerRegistry | None = None,
        auto_start_runner: bool = True,
        log_level: int | None = None,
    ) -> None:
        self.config = config or RuntimeConfig.from_env()
        self.store = store or JobStore(self.config.db_path)
        self.registry = registry or HandlerRegistry()
        if registry is None:
            register_builtin_handlers(self.registry)
        self.runner = JobRunner(self.store, self.registry, self.config)
        self._auto_start = auto_start_runner
        self._started = False
        if log_level is not None:
            configure_logging(log_level)

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #

    async def startup(self) -> None:
        if self._started:
            return
        # Reconcile durable state with this fresh process BEFORE dispatching
        # (see JobStore.recover_on_startup for the exact rules): stale
        # RUNNING rows are finalized, queue/retry clocks are rebased and
        # every unfinished job is handed back to the dispatcher.
        recovered = self.store.recover_on_startup(
            time.monotonic(),
            queue_timeout=self.config.queue_timeout,
            retry_delay_for=self.runner.backoff_delay,
        )
        for job in recovered:
            log_job_event(
                job.id, "recovered",
                to_state=job.state.value,
                detail=(
                    f"attempt={job.attempt}"
                    if job.state is JobState.AWAITING_RETRY
                    else "re-queued"
                ),
            )
        if self._auto_start:
            await self.runner.start()
        self._started = True

    async def shutdown(self) -> None:
        if not self._started:
            return
        await self.runner.stop()
        self._started = False

    # ------------------------------------------------------------------ #
    # operations
    # ------------------------------------------------------------------ #

    async def submit(
        self,
        job_type: str,
        payload: dict[str, Any] | None = None,
        *,
        idempotency_key: str | None = None,
        max_attempts: int | None = None,
        wait_for_slot: float = 2.0,
    ) -> tuple[Job, bool]:
        """Submit a job. Returns ``(job, reused)``.

        Idempotency rules (key present):
        * same key + same payload hash -> existing job, ``reused=True``
        * same key + different payload -> IdempotencyConflictError (409)
        * existing job is terminal     -> TerminalStateConflictError (409)
        """
        payload = dict(payload or {})
        if not self.registry.contains(job_type):
            raise UnknownJobTypeError(
                f"unknown job_type={job_type!r}; "
                "register a handler before submitting"
            )
        attempts = max_attempts or self.config.default_max_attempts
        payload_hash = compute_payload_hash(job_type, payload)

        if idempotency_key is not None:
            existing = self.store.find_idempotent(idempotency_key)
            if existing is not None:
                return self._reject_or_reuse(existing, payload_hash)

        await self._acquire_queue_slot(wait_for_slot)

        now_wall = time.time()
        now_mono = time.monotonic()
        job = Job(
            id=new_job_id(),
            idempotency_key=idempotency_key,
            payload_hash=payload_hash,
            job_type=job_type,
            payload=payload,
            max_attempts=attempts,
            state=JobState.QUEUED,
            created_at=now_wall,
            updated_at=now_wall,
            enqueued_at=now_mono,
            not_before=now_mono,
            deadline_queue=now_mono + self.config.queue_timeout,
        )
        if self.store.insert_job(job):
            log_job_event(
                job.id, "submitted",
                to_state=JobState.QUEUED.value,
                detail=(
                    f"type={job_type} idempotency_key="
                    f"{idempotency_key or '-'} max_attempts={attempts}"
                ),
            )
            return job, False

        # Concurrent submit with the same idempotency key: re-read and apply
        # the exact same reuse/conflict rules deterministically.
        if idempotency_key is not None:
            existing = self.store.find_idempotent(idempotency_key)
            if existing is not None:
                return self._reject_or_reuse(existing, payload_hash)
        raise QueueFullError("insert failed unexpectedly")  # pragma: no cover

    def _reject_or_reuse(
        self, existing: Job, payload_hash: str
    ) -> tuple[Job, bool]:
        # Terminal-state re-operation is checked first: a finished job is
        # never re-executed, regardless of whether the new payload matches,
        # so this rule stays distinguishable from a pure payload conflict.
        if existing.state in TERMINAL_STATES:
            raise TerminalStateConflictError(
                f"job {existing.id} for this idempotency key is already "
                f"terminal (state={existing.state.value}); repeated "
                "submission is rejected"
            )
        if existing.payload_hash != payload_hash:
            log_job_event(
                existing.id, "idempotency_payload_conflict",
                to_state=existing.state.value,
                error_class=ErrorClass.PAYLOAD_CONFLICT.value,
            )
            raise IdempotencyConflictError(
                "idempotency key was already used with a different payload "
                f"(existing job={existing.id})"
            )
        log_job_event(
            existing.id, "idempotent_reuse",
            to_state=existing.state.value,
        )
        return existing, True

    async def _acquire_queue_slot(self, timeout: float) -> None:
        """Wait until QUEUED count < max_queue_size.

        Over-capacity jobs wait briefly here instead of being dropped. If the
        queue stays saturated past ``timeout`` the caller gets QueueFullError.
        """
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            if self.store.count_queue_waiters() < self.config.max_queue_size:
                return
            if time.monotonic() >= deadline:
                raise QueueFullError(
                    "queue is saturated (max_queue_size="
                    f"{self.config.max_queue_size}); try again later"
                )
            await asyncio.sleep(self.config.poll_interval)

    async def get(self, job_id: str) -> Job:
        job = self.store.get(job_id)
        if job is None:
            raise JobNotFoundError(f"job {job_id!r} not found")
        return job

    async def cancel(self, job_id: str, *, signal_runner: bool = True) -> Job:
        """Cancel a non-terminal job.

        QUEUED / AWAITING_RETRY jobs are flipped straight to CANCELLED
        (never executed). A RUNNING job is marked CANCELLED first; if the
        attempt is still in flight it is cancelled via asyncio so its
        coroutine stops promptly — the outcome is then ignored because the
        CAS on RUNNING no longer matches. Terminal jobs raise a
        distinguishable TerminalStateConflictError.
        """
        job = self.store.get(job_id)
        if job is None:
            raise JobNotFoundError(f"job {job_id!r} not found")
        if job.state in TERMINAL_STATES:
            raise TerminalStateConflictError(
                f"job {job_id} is already terminal "
                f"(state={job.state.value}); cancel is rejected"
            )

        ok = self.store.transition(
            job_id,
            [JobState.QUEUED, JobState.AWAITING_RETRY, JobState.RUNNING],
            state=JobState.CANCELLED,
            error_class=ErrorClass.CANCELLED,
            error_message="cancelled by client request",
            finished_at=time.monotonic(),
        )
        if not ok:
            latest = self.store.get(job_id)
            state = latest.state.value if latest is not None else "unknown"
            raise TerminalStateConflictError(
                f"job {job_id} transitioned to {state} "
                "before cancel took effect"
            )
        log_job_event(
            job_id, "cancelled",
            from_state=job.state.value,
            to_state=JobState.CANCELLED.value,
            error_class=ErrorClass.CANCELLED.value,
        )
        if signal_runner and job.state is JobState.RUNNING:
            self.runner.cancel_in_flight(job_id)
        final = self.store.get(job_id)
        assert final is not None  # CAS just succeeded on an existing row
        return final

    async def wait_for_terminal(
        self, job_id: str, timeout: float = 30.0, interval: float = 0.02
    ) -> Job:
        deadline = time.monotonic() + timeout
        while True:
            job = await self.get(job_id)
            if job.state in TERMINAL_STATES:
                return job
            if time.monotonic() >= deadline:
                raise asyncio.TimeoutError(
                    f"job {job_id} did not reach a terminal state within "
                    f"{timeout}s (state={job.state.value})"
                )
            await asyncio.sleep(interval)
