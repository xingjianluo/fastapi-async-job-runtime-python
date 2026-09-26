"""Failure classification and bounded retry scenarios.

Covers:
* non-retryable error fails immediately (attempt == 1);
* retryable error is retried with backoff and exhausted -> FAILED(RETRYABLE);
* flaky handler eventually succeeds, attempt history preserved;
* unexpected exception -> NON_RETRYABLE, immediate failure;
* max_attempts=1 disables retries even for retryable errors;
* results and final state remain queryable after all retries.
"""

import time

import pytest

from job_runtime.models import ErrorClass, JobState


async def test_non_retryable_fails_immediately(make_service):
    svc = await make_service(in_memory=True, default_max_attempts=5,
                             backoff_base_delay=0.01)
    job, _ = await svc.submit("fail_permanent", {}, idempotency_key="perm")
    final = await svc.wait_for_terminal(job.id, timeout=5)

    assert final.state is JobState.FAILED
    assert final.error_class is ErrorClass.NON_RETRYABLE
    assert final.attempt == 1
    assert "permanent" in (final.error_message or "")
    assert len(final.attempt_history) == 1
    assert final.attempt_history[0]["error_class"] == "NON_RETRYABLE"


async def test_retryable_retries_with_backoff_then_fails(make_service):
    svc = await make_service(
        in_memory=True, default_max_attempts=4,
        backoff_base_delay=0.05, backoff_factor=2.0,
    )
    t0 = time.monotonic()
    job, _ = await svc.submit("fail_retryable", {}, idempotency_key="retry")
    final = await svc.wait_for_terminal(job.id, timeout=15)
    elapsed = time.monotonic() - t0

    assert final.state is JobState.FAILED
    assert final.error_class is ErrorClass.RETRYABLE
    assert final.attempt == 4
    # Backoff schedule: 0.05 before attempt 2 + 0.10 before 3 + 0.20 before 4.
    assert elapsed >= 0.30
    classes = {h["error_class"] for h in final.attempt_history}
    assert classes == {"RETRYABLE"}
    assert [h["attempt"] for h in final.attempt_history] == [1, 2, 3, 4]


async def test_flaky_job_succeeds_after_retries(make_service):
    svc = await make_service(in_memory=True, default_max_attempts=5,
                             backoff_base_delay=0.01, backoff_factor=2.0)
    job, _ = await svc.submit(
        "flaky", {"key": "k", "fail_times": 2}, idempotency_key="flaky"
    )
    final = await svc.wait_for_terminal(job.id, timeout=10)

    assert final.state is JobState.SUCCEEDED
    assert final.attempt == 3
    assert final.result == {"succeeded_on_attempt": 3, "key": "k"}
    history = final.attempt_history
    assert [h["outcome"] for h in history] == [
        "ERROR", "ERROR", "SUCCESS"
    ]
    assert [h["error_class"] for h in history[:2]] == [
        "RETRYABLE", "RETRYABLE"
    ]


async def test_unexpected_exception_is_non_retryable(make_service):
    svc = await make_service(in_memory=True, default_max_attempts=5)
    job, _ = await svc.submit("boom", {}, idempotency_key="boom")
    final = await svc.wait_for_terminal(job.id, timeout=5)
    assert final.state is JobState.FAILED
    assert final.error_class is ErrorClass.NON_RETRYABLE
    assert final.attempt == 1
    assert "RuntimeError" in (final.error_message or "")


async def test_single_attempt_disables_retries(make_service):
    svc = await make_service(in_memory=True, backoff_base_delay=0.05)
    job, _ = await svc.submit("fail_retryable", {},
                              idempotency_key="once", max_attempts=1)
    final = await svc.wait_for_terminal(job.id, timeout=5)
    assert final.state is JobState.FAILED
    assert final.error_class is ErrorClass.RETRYABLE
    assert final.attempt == 1
    assert len(final.attempt_history) == 1


async def test_cancel_during_backoff_skips_remaining_retries(make_service):
    # After a retryable failure the job sits in AWAITING_RETRY; cancelling it
    # must win and the scheduled retry must never run.
    svc = await make_service(
        in_memory=True, default_max_attempts=5,
        backoff_base_delay=1.0, backoff_factor=2.0,
    )
    job, _ = await svc.submit("fail_retryable", {}, idempotency_key="backoff")
    # wait until AWAITING_RETRY
    import asyncio
    for _ in range(100):
        cur = await svc.get(job.id)
        if cur.state is JobState.AWAITING_RETRY:
            break
        await asyncio.sleep(0.01)
    assert (await svc.get(job.id)).state is JobState.AWAITING_RETRY

    cancelled = await svc.cancel(job.id)
    assert cancelled.state is JobState.CANCELLED
    await asyncio.sleep(1.4)  # longer than the first backoff
    cur = await svc.get(job.id)
    assert cur.state is JobState.CANCELLED
    assert cur.attempt == 1  # retry never happened
