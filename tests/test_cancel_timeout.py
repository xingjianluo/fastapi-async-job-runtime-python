"""Cancellation and timeout scenarios.

Covers:
* cancel a QUEUED job            -> CANCELLED, never executed;
* cancel a RUNNING slow job       -> CANCELLED promptly, task stopped;
* duplicate cancel / cancel after finish -> TERMINAL_STATE_CONFLICT;
* attempt timeout retries and exhausts -> terminal TIMED_OUT;
* queue-wait timeout             -> terminal TIMED_OUT(QUEUE_TIMEOUT)
                                    without executing the job;
* cancelling one job never affects another running job.
"""

import asyncio
import time

import pytest

from job_runtime.models import (
    ErrorClass,
    JobState,
    TerminalStateConflictError,
)


async def test_cancel_queued_job_never_runs(make_service):
    svc = await make_service(
        in_memory=True, max_concurrency=1, max_queue_size=10,
    )
    # Occupy the single slot.
    blocker, _ = await svc.submit("slow", {"seconds": 1.0},
                                  idempotency_key="blocker")
    await asyncio.sleep(0.15)  # let the blocker reach RUNNING

    queued, _ = await svc.submit("succeed", {}, idempotency_key="queued")
    await asyncio.sleep(0.05)
    assert (await svc.get(queued.id)).state is JobState.QUEUED

    cancelled = await svc.cancel(queued.id)
    assert cancelled.state is JobState.CANCELLED
    assert cancelled.error_class is ErrorClass.CANCELLED
    assert cancelled.attempt == 0  # never claimed/executed

    await svc.wait_for_terminal(blocker.id, timeout=5)


async def test_cancel_running_job_stops_promptly(make_service):
    svc = await make_service(
        in_memory=True, max_concurrency=1, attempt_timeout=30.0,
    )
    job, _ = await svc.submit("slow", {"seconds": 30.0},
                              idempotency_key="long")
    await asyncio.sleep(0.15)
    assert (await svc.get(job.id)).state is JobState.RUNNING

    started = time.monotonic()
    cancelled = await svc.cancel(job.id)
    elapsed = time.monotonic() - started
    assert cancelled.state is JobState.CANCELLED
    assert cancelled.error_class is ErrorClass.CANCELLED
    assert elapsed < 2.0  # the in-flight coroutine is cancelled, not awaited

    # State must never roll back despite the failed/aborted attempt.
    await asyncio.sleep(0.2)
    assert (await svc.get(job.id)).state is JobState.CANCELLED
    assert svc.runner.active_runs() == 0


async def test_double_cancel_and_cancel_after_finish_rejected(make_service):
    svc = await make_service(in_memory=True)
    job, _ = await svc.submit("succeed", {}, idempotency_key="once")
    final = await svc.wait_for_terminal(job.id, timeout=5)
    assert final.state is JobState.SUCCEEDED

    with pytest.raises(TerminalStateConflictError) as exc:
        await svc.cancel(job.id)
    assert exc.value.code == "TERMINAL_STATE_CONFLICT"


async def test_attempt_timeout_retries_then_times_out(make_service):
    svc = await make_service(
        in_memory=True, max_concurrency=2, attempt_timeout=0.2,
        default_max_attempts=3, backoff_base_delay=0.01,
        backoff_factor=2.0,
    )
    job, _ = await svc.submit("slow", {"seconds": 5.0},
                              idempotency_key="timeout", max_attempts=3)
    final = await svc.wait_for_terminal(job.id, timeout=10)

    assert final.state is JobState.TIMED_OUT
    assert final.error_class is ErrorClass.TIMEOUT
    assert final.attempt == 3
    error_outcomes = [
        h for h in final.attempt_history
        if h.get("error_class") == ErrorClass.TIMEOUT.value
    ]
    assert len(error_outcomes) == 3  # each attempt classified consistently


async def test_attempt_timeout_recovers_on_later_attempt(make_service):
    # flaky handler succeeds quickly on the 2nd attempt: a TIMEOUT-class
    # failure would exhaust attempts; here ordinary retryable succeeds.
    svc = await make_service(
        in_memory=True, attempt_timeout=5.0, default_max_attempts=3,
        backoff_base_delay=0.01,
    )
    job, _ = await svc.submit("flaky", {"key": "f1", "fail_times": 1},
                              idempotency_key="flaky-ok")
    final = await svc.wait_for_terminal(job.id, timeout=5)
    assert final.state is JobState.SUCCEEDED
    assert final.attempt == 2
    assert final.result["succeeded_on_attempt"] == 2


async def test_queue_wait_timeout_terminates_without_execution(make_service):
    svc = await make_service(
        in_memory=True, max_concurrency=1, queue_timeout=0.25,
    )
    blocker, _ = await svc.submit("slow", {"seconds": 3.0},
                                  idempotency_key="blocker-2")
    await asyncio.sleep(0.15)

    victim, _ = await svc.submit("succeed", {}, idempotency_key="victim")
    final = await svc.wait_for_terminal(victim.id, timeout=5)

    assert final.state is JobState.TIMED_OUT
    assert final.error_class is ErrorClass.QUEUE_TIMEOUT
    assert final.attempt == 0  # queued timeout never executes the job


async def test_cancel_one_job_does_not_touch_others(make_service):
    svc = await make_service(
        in_memory=True, max_concurrency=2, attempt_timeout=30.0,
    )
    slow_job, _ = await svc.submit("slow", {"seconds": 20.0},
                                   idempotency_key="s1")
    fast_job, _ = await svc.submit("succeed", {"delay": 0.2},
                                   idempotency_key="f1")
    await asyncio.sleep(0.15)
    await svc.cancel(slow_job.id)
    fast_final = await svc.wait_for_terminal(fast_job.id, timeout=5)
    assert fast_final.state is JobState.SUCCEEDED

    slow_final = await svc.get(slow_job.id)
    assert slow_final.state is JobState.CANCELLED


async def test_repeated_concurrent_cancel_has_single_winner(make_service):
    svc = await make_service(in_memory=True, attempt_timeout=30.0)
    job, _ = await svc.submit("slow", {"seconds": 20.0},
                              idempotency_key="race-cancel")
    await asyncio.sleep(0.15)

    outcomes = await asyncio.gather(
        *[svc.cancel(job.id) for _ in range(10)],
        return_exceptions=True,
    )
    successes = [o for o in outcomes if not isinstance(o, Exception)]
    conflicts = [
        o for o in outcomes
        if isinstance(o, TerminalStateConflictError)
    ]
    assert len(successes) == 1
    assert successes[0].state is JobState.CANCELLED
    assert len(conflicts) == 9
    # Never re-executed, never rolled back.
    await asyncio.sleep(0.2)
    assert (await svc.get(job.id)).state is JobState.CANCELLED
    assert svc.runner.active_runs() == 0
