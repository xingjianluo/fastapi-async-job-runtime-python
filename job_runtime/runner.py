"""The runtime scheduler: bounded concurrency, queueing, timeouts, retries.

Design notes
------------
* One :class:`JobRunner` owns execution per process.
* Free slots enforce ``max_concurrency``; due jobs are pulled from the
  durable store by a single dispatcher coroutine, so each job is launched
  at most once in this process. The SQLite CAS transition
  (``QUEUED/AWAITING_RETRY -> RUNNING``) is the second, restart-safe guard.
* All scheduling timestamps (``not_before``, ``deadline_queue``) use
  :func:`time.monotonic`; wall-clock ``created_at`` etc. are only for
  presentation and live on the Job dataclass.
* Per-attempt timeout uses ``asyncio.wait_for``. Timeouts count as
  retryable (ErrorClass.TIMEOUT); once attempts are exhausted the job ends
  in terminal TIMED_OUT.
* Queue wait timeout (ErrorClass.QUEUE_TIMEOUT) moves an overlong-waiting
  QUEUED job straight to terminal TIMED_OUT without executing it.
* Cancellation wins deterministically: a terminal DB state always beats an
  in-flight attempt result; cancelling before execution skips the attempt.
"""

from __future__ import annotations

import asyncio
import time
import traceback
from typing import Any

from .config import RuntimeConfig
from .handlers import HandlerRegistry, JobExecutionError
from .log import log_job_event
from .models import ErrorClass, JobState, TERMINAL_STATES
from .storage import JobStore

_TERMINAL = TERMINAL_STATES


class JobRunner:
    def __init__(
        self,
        store: JobStore,
        registry: HandlerRegistry,
        config: RuntimeConfig,
    ) -> None:
        self.store = store
        self.registry = registry
        self.config = config
        self._tasks: set[asyncio.Task] = set()
        self._task_by_job: dict[str, asyncio.Task] = {}
        self._dispatch_task: asyncio.Task | None = None
        self._stopping = asyncio.Event()
        self._in_flight: set[str] = set()

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        if self._dispatch_task is None:
            self._stopping.clear()
            self._dispatch_task = asyncio.create_task(self._dispatch_loop())
            log_job_event("-", "runner_started",
                          detail=f"max_concurrency={self.config.max_concurrency}")

    async def stop(self) -> None:
        """Stop dispatching and cancel in-flight attempts.

        Attempts are not marked terminal here: on restart the store treats
        stale RUNNING rows as FAILED(INTERRUPTED), so no job is executed
        twice and none is silently lost.
        """
        self._stopping.set()
        # Record an orderly shutdown *before* aborting attempts: if the
        # process is restarted, stale RUNNING rows become CANCELLED rather
        # than FAILED(INTERRUPTED) (which describes a hard crash).
        self.store.mark_orderly_shutdown(time.monotonic())
        if self._dispatch_task is not None:
            self._dispatch_task.cancel()
            try:
                await self._dispatch_task
            except asyncio.CancelledError:
                pass
            self._dispatch_task = None
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._task_by_job.clear()
        self._in_flight.clear()
        log_job_event("-", "runner_stopped")

    def active_runs(self) -> int:
        return len(self._tasks)

    def cancel_in_flight(self, job_id: str) -> bool:
        """Cancel the in-flight attempt task for ``job_id`` if present.

        State is already CANCELLED in the database (set by the service
        before this call); the attempt result will be discarded by the
        RUNNING-only CAS. Returns False if no task is executing the job.
        """
        task = self._task_by_job.get(job_id)
        if task is not None and not task.done():
            task.cancel()
            return True
        return False

    def _on_task_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        for job_id, candidate in list(self._task_by_job.items()):
            if candidate is task:
                del self._task_by_job[job_id]

    # ------------------------------------------------------------------ #
    # dispatcher
    # ------------------------------------------------------------------ #

    async def _dispatch_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                launched = await self._dispatch_tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # defensive: keep the loop alive
                log_job_event("-", "dispatcher_error", detail=traceback.format_exc())
                launched = False
            await asyncio.sleep(0 if launched else self.config.poll_interval)

    async def _dispatch_tick(self) -> bool:
        """Expire stale queue entries, then launch due jobs into free slots."""
        now = time.monotonic()

        for dead in self.store.expire_queue_timeouts(now):
            log_job_event(
                dead.id, "queue_timeout",
                from_state=JobState.QUEUED.value,
                to_state=JobState.TIMED_OUT.value,
                error_class=ErrorClass.QUEUE_TIMEOUT.value,
            )

        free_slots = self.config.max_concurrency - len(self._tasks)
        if free_slots <= 0:
            return False

        due = self.store.list_due(
            now, limit=free_slots + len(self._in_flight)
        )
        launched = False
        for job in due:
            if free_slots <= 0:
                break
            if job.id in self._in_flight:
                continue  # already executing in this process

            prev_state = job.state
            # Authoritative claim guard. A job cancelled or queue-expired
            # between the read and this UPDATE is simply skipped.
            ok = self.store.transition(
                job.id,
                [JobState.QUEUED, JobState.AWAITING_RETRY],
                state=JobState.RUNNING,
                attempt=job.attempt + 1,
                started_at=now,
            )
            if not ok:
                continue

            self._in_flight.add(job.id)
            claimed = self.store.get(job.id)
            task = asyncio.create_task(self._guarded_run(job.id))
            self._tasks.add(task)
            self._task_by_job[job.id] = task
            task.add_done_callback(self._on_task_done)
            free_slots -= 1
            launched = True
            log_job_event(
                job.id, "claimed",
                from_state=prev_state.value,
                to_state=JobState.RUNNING.value,
                attempt=claimed.attempt if claimed else None,
            )
        return launched

    # ------------------------------------------------------------------ #
    # execution
    # ------------------------------------------------------------------ #

    def backoff_delay(self, attempt: int) -> float:
        """Delay before attempt ``attempt`` (1-based). Attempt 1 has none."""
        if attempt <= 1:
            return 0.0
        delay = self.config.backoff_base_delay * (
            self.config.backoff_factor ** (attempt - 2)
        )
        return min(delay, self.config.backoff_max_delay)

    async def _guarded_run(self, job_id: str) -> None:
        try:
            await self._run_job(job_id)
        except asyncio.CancelledError:
            log_job_event(job_id, "attempt_cancelled_by_shutdown")
            raise
        except Exception:  # defensive: a bug here must not kill the runner
            log_job_event(job_id, "runner_bug", detail=traceback.format_exc())
            self.store.transition(
                job_id, [JobState.RUNNING],
                state=JobState.FAILED,
                error_class=ErrorClass.NON_RETRYABLE,
                error_message="runner internal error",
                finished_at=time.monotonic(),
            )
        finally:
            self._in_flight.discard(job_id)

    async def _run_job(self, job_id: str) -> None:
        job = self.store.get(job_id)
        if job is None or job.state is not JobState.RUNNING:
            return
        handler = self.registry.get(job.job_type)
        attempt_no = job.attempt
        started = time.monotonic()

        if handler is None:  # defensive: submit() validates job types
            self._apply_failure(
                job, attempt_no, started,
                ErrorClass.NON_RETRYABLE,
                f"no handler registered for job_type={job.job_type!r}",
            )
            return

        try:
            result = await asyncio.wait_for(
                handler(job_id, job.payload),
                timeout=self.config.attempt_timeout,
            )
        except asyncio.TimeoutError:
            self._apply_failure(
                job, attempt_no, started, ErrorClass.TIMEOUT,
                f"attempt {attempt_no} exceeded "
                f"{self.config.attempt_timeout:.3f}s execution timeout",
            )
            return
        except JobExecutionError as exc:
            self._apply_failure(
                job, attempt_no, started, exc.error_class, exc.message
            )
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._apply_failure(
                job, attempt_no, started, ErrorClass.NON_RETRYABLE,
                f"unexpected {type(exc).__name__}: {exc}",
            )
            return

        ok = self.store.transition(
            job_id, [JobState.RUNNING],
            state=JobState.SUCCEEDED,
            error_class=ErrorClass.NONE,
            error_message=None,
            result=result,
            finished_at=time.monotonic(),
            append_history=self._history_entry(attempt_no, "SUCCESS",
                                               started, None),
        )
        if ok:
            log_job_event(
                job_id, "succeeded",
                from_state=JobState.RUNNING.value,
                to_state=JobState.SUCCEEDED.value,
                attempt=attempt_no,
                error_class=ErrorClass.NONE.value,
            )
        else:
            log_job_event(
                job_id, "success_after_terminal_ignored",
                attempt=attempt_no,
                detail=self._terminal_state(job_id),
            )

    def _apply_failure(
        self,
        job: Any,
        attempt_no: int,
        started: float,
        error_class: ErrorClass,
        message: str,
    ) -> None:
        """Record the failed attempt, then retry-with-backoff or terminate.

        Every write is a CAS on RUNNING: a concurrently cancelled job stays
        CANCELLED and the failure result is ignored (terminal states never
        roll back).
        """
        job_id = job.id
        history_entry = {
            "attempt": attempt_no,
            "outcome": "ERROR",
            "error_class": error_class.value,
            "started_at": started,
            "ended_at": time.monotonic(),
            "message": message,
        }

        retriable = error_class in (ErrorClass.RETRYABLE, ErrorClass.TIMEOUT)
        if retriable and attempt_no < job.max_attempts:
            not_before = time.monotonic() + self.backoff_delay(attempt_no + 1)
            ok = self.store.transition(
                job_id, [JobState.RUNNING],
                state=JobState.AWAITING_RETRY,
                error_class=error_class,
                error_message=message,
                attempt=attempt_no,
                not_before=not_before,
                append_history=history_entry,
            )
            if ok:
                log_job_event(
                    job_id, "retry_scheduled",
                    from_state=JobState.RUNNING.value,
                    to_state=JobState.AWAITING_RETRY.value,
                    attempt=attempt_no,
                    error_class=error_class.value,
                    detail=(
                        f"next_attempt={attempt_no + 1} "
                        f"retry_after={not_before - time.monotonic():.3f}s"
                    ),
                )
            else:
                log_job_event(
                    job_id, "retry_blocked_by_terminal",
                    attempt=attempt_no,
                    detail=self._terminal_state(job_id),
                )
            return

        terminal = (
            JobState.TIMED_OUT if error_class is ErrorClass.TIMEOUT
            else JobState.FAILED
        )
        ok = self.store.transition(
            job_id, [JobState.RUNNING],
            state=terminal,
            error_class=error_class,
            error_message=message,
            finished_at=time.monotonic(),
            append_history=history_entry,
        )
        if ok:
            log_job_event(
                job_id,
                "retries_exhausted" if retriable else "failed_permanent",
                from_state=JobState.RUNNING.value,
                to_state=terminal.value,
                attempt=attempt_no,
                error_class=error_class.value,
                detail=message,
            )
        else:
            log_job_event(
                job_id, "failure_after_terminal_ignored",
                attempt=attempt_no,
                error_class=error_class.value,
                detail=self._terminal_state(job_id),
            )

    # ------------------------------------------------------------------ #
    # misc
    # ------------------------------------------------------------------ #

    @staticmethod
    def _history_entry(
        attempt: int, outcome: str, started: float, message: str | None,
    ) -> dict[str, Any]:
        return {
            "attempt": attempt,
            "outcome": outcome,
            "started_at": started,
            "ended_at": time.monotonic(),
            "message": message,
        }

    def _terminal_state(self, job_id: str) -> str:
        latest = self.store.get(job_id)
        if latest is None:
            return "job_missing"
        if latest.state in _TERMINAL:
            return f"terminal={latest.state.value}"
        return f"state={latest.state.value}"
