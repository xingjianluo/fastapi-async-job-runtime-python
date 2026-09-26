"""Concurrent submission and idempotency scenarios.

Covers:
* concurrent submissions sharing one idempotency key -> exactly one job,
  every caller receives the same job id;
* same key + different payload -> 409 IDEMPOTENCY_PAYLOAD_CONFLICT;
* re-submitting a terminal job with the same key -> 409
  TERMINAL_STATE_CONFLICT (distinct from payload conflict);
* bounded queue: over-capacity jobs wait and are never silently dropped.
"""

import asyncio

import pytest

from job_runtime.models import (
    IdempotencyConflictError,
    JobState,
    TerminalStateConflictError,
)


async def test_concurrent_submissions_same_key_return_single_job(make_service):
    svc = await make_service(
        in_memory=True, max_concurrency=4, attempt_timeout=60.0
    )
    key = "concurrent-key-1"

    # All callers race on the same key while the (slow) first job is still
    # active, so the outcome is independent of completion timing.
    results = await asyncio.gather(*[
        svc.submit("slow", {"seconds": 30, "shared": True},
                   idempotency_key=key)
        for _ in range(20)
    ])

    ids = {job.id for job, _ in results}
    assert len(ids) == 1, f"expected exactly one job, got {ids}"
    reused_flags = [reused for _, reused in results]
    assert reused_flags.count(False) == 1
    assert reused_flags.count(True) == 19

    job_id = ids.pop()
    cancelled = await svc.cancel(job_id)
    assert cancelled.state is JobState.CANCELLED


async def test_same_key_different_payload_is_rejected(make_service):
    svc = await make_service(in_memory=True, attempt_timeout=30.0)
    # slow keeps the first job non-terminal for the whole test.
    job, reused = await svc.submit(
        "slow", {"seconds": 30, "a": 1}, idempotency_key="K"
    )
    assert not reused

    with pytest.raises(IdempotencyConflictError) as exc:
        await svc.submit("slow", {"seconds": 30, "a": 2},
                         idempotency_key="K")
    assert exc.value.code == "IDEMPOTENCY_PAYLOAD_CONFLICT"
    assert exc.value.status_code == 409
    assert job.id in exc.value.message

    # A different job_type with the same body is also a payload-hash conflict.
    with pytest.raises(IdempotencyConflictError):
        await svc.submit("fail_permanent", {"seconds": 30, "a": 1},
                         idempotency_key="K")

    # The conflicting submission must not have created a second job.
    assert svc.store.find_idempotent("K").id == job.id
    await svc.cancel(job.id)


async def test_terminal_job_resubmit_is_distinct_conflict(make_service):
    svc = await make_service(in_memory=True)
    job, _ = await svc.submit("fail_permanent", {}, idempotency_key="TERM")
    final = await svc.wait_for_terminal(job.id, timeout=5)
    assert final.state is JobState.FAILED

    with pytest.raises(TerminalStateConflictError) as exc:
        await svc.submit("fail_permanent", {}, idempotency_key="TERM")
    assert exc.value.code == "TERMINAL_STATE_CONFLICT"
    assert "already terminal" in exc.value.message


async def test_successful_terminal_resubmit_rejected_with_reason(make_service):
    svc = await make_service(in_memory=True)
    job, _ = await svc.submit("succeed", {}, idempotency_key="DONE")
    await svc.wait_for_terminal(job.id, timeout=5)

    with pytest.raises(TerminalStateConflictError) as exc:
        await svc.submit("succeed", {}, idempotency_key="DONE")
    assert exc.value.code == "TERMINAL_STATE_CONFLICT"
    assert exc.value.status_code == 409


async def test_terminal_conflict_takes_priority_over_payload_conflict(
    make_service
):
    svc = await make_service(in_memory=True)
    job, _ = await svc.submit("succeed", {"v": 1}, idempotency_key="PRIO")
    await svc.wait_for_terminal(job.id, timeout=5)

    # Same key, different payload, but job already terminal: terminal rule wins.
    with pytest.raises(TerminalStateConflictError) as exc:
        await svc.submit("succeed", {"v": 2}, idempotency_key="PRIO")
    assert exc.value.code == "TERMINAL_STATE_CONFLICT"


async def test_unique_keys_run_independently(make_service):
    svc = await make_service(in_memory=True, max_concurrency=4)
    submitted = [
        await svc.submit("succeed", {"i": i}, idempotency_key=f"u-{i}")
        for i in range(5)
    ]
    assert len({j.id for j, _ in submitted}) == 5
    finals = await asyncio.gather(*[
        svc.wait_for_terminal(j.id, timeout=5) for j, _ in submitted
    ])
    assert all(j.state is JobState.SUCCEEDED for j in finals)


async def test_bounded_queue_waits_instead_of_dropping(make_service):
    # concurrency=1, small in-memory queue: extra jobs must wait in QUEUED
    # and all complete successfully (nothing dropped).
    svc = await make_service(
        in_memory=True, max_concurrency=1, max_queue_size=2,
    )
    jobs = []
    # First job occupies the slot; next two fill the queue; the 4th blocks
    # briefly in _acquire_queue_slot until a slot frees.
    jobs_resp = await asyncio.gather(*[
        svc.submit("succeed", {"delay": 0.15}, idempotency_key=f"q-{i}")
        for i in range(4)
    ])
    jobs = [j for j, _ in jobs_resp]
    assert len({j.id for j in jobs}) == 4

    finals = await asyncio.gather(*[
        svc.wait_for_terminal(j.id, timeout=10) for j in jobs
    ])
    assert [j.state for j in finals] == [JobState.SUCCEEDED] * 4


async def test_unknown_job_type_rejected(make_service):
    from job_runtime.models import UnknownJobTypeError

    svc = await make_service(in_memory=True, auto_start_runner=False)
    with pytest.raises(UnknownJobTypeError) as exc:
        await svc.submit("does-not-exist", {})
    assert exc.value.code == "UNKNOWN_JOB_TYPE"
    assert exc.value.status_code == 422
