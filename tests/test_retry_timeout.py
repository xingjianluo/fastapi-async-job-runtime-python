"""失败分类、有限退避重试、重试耗尽、执行超时与排队超时。"""

from __future__ import annotations

import asyncio

import pytest
from conftest import wait_for

from app.models import JobState, JobSubmitRequest


@pytest.mark.asyncio
async def test_retryable_succeeds_within_attempts(manager):
    job, _ = await manager.submit(
        JobSubmitRequest(job_type="flaky", payload={"fail_times": 2})
    )
    v = await wait_for(manager, job.id, {JobState.SUCCEEDED})
    assert v["attempt"] == 3
    kinds = [
        a["result"]["failure_kind"] if a["result"] and not a["result"]["ok"] else None
        for a in v["attempts"]
    ]
    assert kinds[:2] == ["RETRYABLE", "RETRYABLE"]
    assert v["attempts"][-1]["result"]["ok"] is True
    # 退避后再次执行，中间状态出现过 RETRY_WAIT
    assert any(a["state_after"] == "RETRY_WAIT" for a in v["attempts"][:-1])


@pytest.mark.asyncio
async def test_retryable_exhausted_fails_terminal(manager):
    job, _ = await manager.submit(
        JobSubmitRequest(job_type="fail", payload={"kind": "retryable"})
    )
    v = await wait_for(manager, job.id, {JobState.FAILED})
    assert v["state"] == "FAILED"
    assert v["error_code"] == "RETRYABLE_FAILURE"
    assert v["failure_kind"] == "RETRYABLE"
    assert v["attempt"] == 3  # max_attempts
    assert len(v["attempts"]) == 3
    assert v["terminal"] is True


@pytest.mark.asyncio
async def test_non_retryable_fails_immediately(manager):
    job, _ = await manager.submit(
        JobSubmitRequest(job_type="fail", payload={"message": "boom"})
    )
    v = await wait_for(manager, job.id, {JobState.FAILED})
    assert v["state"] == "FAILED"
    assert v["error_code"] == "NON_RETRYABLE_FAILURE"
    assert v["failure_kind"] == "NON_RETRYABLE"
    assert len(v["attempts"]) == 1  # 不重试


@pytest.mark.asyncio
async def test_non_retryable_explicit_error(manager):
    job, _ = await manager.submit(
        JobSubmitRequest(job_type="flaky", payload={"non_retryable": True})
    )
    v = await wait_for(manager, job.id, {JobState.FAILED})
    assert v["error_code"] == "NON_RETRYABLE_FAILURE"
    assert len(v["attempts"]) == 1


@pytest.mark.asyncio
async def test_run_timeout_retries_then_terminal_timeout(manager):
    # run_timeout=0.05，sleep 2 秒：每次尝试都超时，耗尽后 TIMED_OUT。
    job, _ = await manager.submit(
        JobSubmitRequest(
            job_type="sleep", payload={"seconds": 2.0}, run_timeout=0.05
        )
    )
    v = await wait_for(manager, job.id, {JobState.TIMED_OUT}, timeout=5)
    assert v["state"] == "TIMED_OUT"
    assert v["error_code"] == "ATTEMPT_TIMEOUT"
    assert v["failure_kind"] == "TIMEOUT"
    assert v["attempt"] == 3
    # 每次尝试的失败分类都应是 TIMEOUT
    assert all(
        a["result"]["failure_kind"] == "TIMEOUT" for a in v["attempts"]
    )


@pytest.mark.asyncio
async def test_queue_timeout_marks_terminal(tmp_path):
    from app.config import Settings
    from app.handlers import default_handlers
    from app.manager import JobManager
    from app.storage import SqliteJobStorage

    db = str(tmp_path / "qt.db")
    settings = Settings(
        db_path=db, max_concurrency=1, max_queue_size=10, queue_timeout=0.3,
        run_timeout=5, max_attempts=1, retry_base_delay=0.01,
        retry_max_delay=0.01, poll_interval=0.02,
    )
    storage = SqliteJobStorage(db)
    mgr = JobManager(storage, default_handlers(), settings)
    await mgr.start()
    try:
        # 占住唯一执行槽很久，使后续作业的排队截止时间先到
        blocker, _ = await mgr.submit(
            JobSubmitRequest(job_type="sleep", payload={"seconds": 3.0})
        )
        queued, _ = await mgr.submit(
            JobSubmitRequest(job_type="echo", payload={}, queue_timeout=0.2)
        )
        v = await wait_for(mgr, queued.id, {JobState.TIMED_OUT}, timeout=3)
        assert v["error_code"] == "QUEUE_TIMEOUT"
        assert v["failure_kind"] == "TIMEOUT"
        assert v["terminal"] is True
        # blocker 未受影响
        b = await wait_for(mgr, blocker.id, {JobState.SUCCEEDED}, timeout=6)
        assert b["state"] == "SUCCEEDED"
    finally:
        await mgr.shutdown()
        storage.close()


@pytest.mark.asyncio
async def test_retry_backoff_is_bounded_and_deterministic(manager):
    # fail_times=1，base=0.03：第二次应在约 0.03s 后重试，最终成功
    loop = asyncio.get_running_loop()
    job, _ = await manager.submit(
        JobSubmitRequest(job_type="flaky", payload={"fail_times": 1})
    )
    start = loop.time()
    v = await wait_for(manager, job.id, {JobState.SUCCEEDED}, timeout=5)
    elapsed = loop.time() - start
    assert v["attempt"] == 2
    assert elapsed >= 0.03 - 0.02  # 至少经历退避
