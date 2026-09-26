"""Restart durability and recovery scenarios.

Covers:
* terminal (SUCCEEDED/FAILED) results survive a service restart and stay
  queryable with full attempt history;
* a job left RUNNING when the process died is finalized as
  FAILED(INTERRUPTED) and never executed twice;
* QUEUED jobs resume after restart and complete normally (no loss);
* AWAITING_RETRY jobs resume their remaining attempts after restart.
"""

import asyncio

import pytest

from job_runtime.models import ErrorClass, JobState


async def test_terminal_results_survive_restart(make_service, tmp_db):
    svc = await make_service(db_path=tmp_db, default_max_attempts=3,
                             backoff_base_delay=0.01)
    ok_job, _ = await svc.submit("succeed", {"v": 42},
                                 idempotency_key="persist-ok")
    bad_job, _ = await svc.submit("fail_permanent", {},
                                  idempotency_key="persist-bad")
    ok_final = await svc.wait_for_terminal(ok_job.id, timeout=5)
    bad_final = await svc.wait_for_terminal(bad_job.id, timeout=5)
    assert ok_final.state is JobState.SUCCEEDED
    assert bad_final.state is JobState.FAILED
    await svc.shutdown()
    svc.store.close()

    svc2 = await make_service(db_path=tmp_db, auto_start_runner=False)
    reloaded_ok = await svc2.get(ok_job.id)
    reloaded_bad = await svc2.get(bad_job.id)

    assert reloaded_ok.state is JobState.SUCCEEDED
    assert reloaded_ok.result == {"echo": {"v": 42}, "job_id": ok_job.id}
    assert reloaded_bad.state is JobState.FAILED
    assert reloaded_bad.error_class is ErrorClass.NON_RETRYABLE
    assert len(reloaded_bad.attempt_history) == 1

    # Idempotency table survives too: repeat submit still rejected.
    from job_runtime.models import TerminalStateConflictError
    with pytest.raises(TerminalStateConflictError):
        await svc2.submit("succeed", {"v": 42},
                          idempotency_key="persist-ok")


async def test_stale_running_job_finalized_interrupted_and_not_rerun(
    make_service, tmp_db
):
    # First process: claim a job into RUNNING, then "die" by closing the
    # store without stopping the runner cleanly.
    svc = await make_service(db_path=tmp_db, max_concurrency=4,
                             attempt_timeout=60.0)
    job, _ = await svc.submit("succeed", {"delay": 30.0},
                              idempotency_key="running-die")
    for _ in range(200):
        cur = await svc.get(job.id)
        if cur.state is JobState.RUNNING:
            break
        await asyncio.sleep(0.01)
    assert (await svc.get(job.id)).state is JobState.RUNNING

    # Simulate a hard process crash: close DB connection without shutdown.
    svc.store.close()

    # Second process opens the same DB file.
    svc2 = await make_service(db_path=tmp_db, auto_start_runner=True)
    recovered = await svc2.get(job.id)
    assert recovered.state is JobState.FAILED
    assert recovered.error_class is ErrorClass.INTERRUPTED
    assert recovered.finished_at is not None
    # It must not be re-executed later.
    await asyncio.sleep(0.2)
    assert (await svc2.get(job.id)).state is JobState.FAILED


async def test_queued_job_resumes_after_restart(make_service, tmp_db):
    # Process 1: concurrency 1 with a blocker; the victim stays QUEUED,
    # then process disappears.
    svc = await make_service(db_path=tmp_db, max_concurrency=1,
                             queue_timeout=3600.0)
    blocker, _ = await svc.submit("slow", {"seconds": 20.0},
                                  idempotency_key="r-blocker")
    victim, _ = await svc.submit("succeed", {"x": "after-restart"},
                                 idempotency_key="r-victim")
    await asyncio.sleep(0.2)
    assert (await svc.get(victim.id)).state is JobState.QUEUED
    await svc.shutdown()
    svc.store.close()

    # Process 2: free concurrency — the queued job must run to success.
    svc2 = await make_service(db_path=tmp_db, max_concurrency=4,
                              queue_timeout=3600.0)
    final = await svc2.wait_for_terminal(victim.id, timeout=10)
    assert final.state is JobState.SUCCEEDED
    assert final.result["echo"] == {"x": "after-restart"}


async def test_awaiting_retry_job_resumes_after_restart(
    make_service, tmp_db
):
    svc = await make_service(
        db_path=tmp_db, default_max_attempts=3,
        backoff_base_delay=30.0, backoff_max_delay=30.0,
    )
    job, _ = await svc.submit("fail_retryable", {},
                              idempotency_key="r-retry")
    for _ in range(300):
        cur = await svc.get(job.id)
        if cur.state is JobState.AWAITING_RETRY:
            break
        await asyncio.sleep(0.01)
    assert cur.state is JobState.AWAITING_RETRY
    assert cur.attempt == 1
    await svc.shutdown()
    svc.store.close()

    # Process 2: short backoff configured; due retries must resume and
    # exhaust, ending FAILED(RETRYABLE) with exactly 3 attempts — never
    # reset to attempt 1, never executed twice concurrently.
    svc2 = await make_service(
        db_path=tmp_db, default_max_attempts=3,
        backoff_base_delay=0.01, backoff_factor=2.0,
    )
    final = await svc2.wait_for_terminal(job.id, timeout=10)
    assert final.state is JobState.FAILED
    assert final.error_class is ErrorClass.RETRYABLE
    assert final.attempt == 3
    assert [h["attempt"] for h in final.attempt_history] == [1, 2, 3]


async def test_orderly_shutdown_marks_running_as_cancelled(
    make_service, tmp_db
):
    # Process 1 stops the runner deliberately while a job is RUNNING.
    svc = await make_service(db_path=tmp_db, attempt_timeout=60.0)
    job, _ = await svc.submit("slow", {"seconds": 30.0},
                              idempotency_key="graceful")
    for _ in range(200):
        cur = await svc.get(job.id)
        if cur.state is JobState.RUNNING:
            break
        await asyncio.sleep(0.01)
    assert cur.state is JobState.RUNNING
    await svc.shutdown()          # orderly: writes shutdown marker
    svc.store.close()

    # Process 2: stale RUNNING row is CANCELLED, not INTERRUPTED.
    svc2 = await make_service(db_path=tmp_db)
    reloaded = await svc2.get(job.id)
    assert reloaded.state is JobState.CANCELLED
    assert reloaded.error_class is ErrorClass.CANCELLED
    assert "shutdown" in (reloaded.error_message or "")


async def test_marker_consumed_then_crash_is_interrupted_again(
    make_service, tmp_db
):
    # After one orderly recovery (marker consumed), a second process that
    # crashes mid-run must be classified INTERRUPTED again.
    svc = await make_service(db_path=tmp_db, attempt_timeout=60.0)
    job, _ = await svc.submit("slow", {"seconds": 30.0},
                              idempotency_key="twice")
    for _ in range(200):
        if (await svc.get(job.id)).state is JobState.RUNNING:
            break
        await asyncio.sleep(0.01)
    await svc.shutdown()
    svc.store.close()

    svc2 = await make_service(db_path=tmp_db, attempt_timeout=60.0)
    # The recovered job was CANCELLED; submit a fresh one and hard-crash.
    assert (await svc2.get(job.id)).state is JobState.CANCELLED
    job2, _ = await svc2.submit("slow", {"seconds": 30.0},
                                idempotency_key="twice-2")
    for _ in range(200):
        if (await svc2.get(job2.id)).state is JobState.RUNNING:
            break
        await asyncio.sleep(0.01)
    svc2.store.close()  # hard crash, no marker

    svc3 = await make_service(db_path=tmp_db)
    reloaded = await svc3.get(job2.id)
    assert reloaded.state is JobState.FAILED
    assert reloaded.error_class is ErrorClass.INTERRUPTED
