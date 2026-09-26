"""有界并发、排队等待与取消（排队中/执行中/终态拒绝）。"""

from __future__ import annotations

import asyncio

import pytest
from conftest import wait_for

from app.config import Settings
from app.errors import AlreadyTerminal, ErrorCode, QueueFull
from app.handlers import default_handlers
from app.manager import JobManager
from app.models import JobState, JobSubmitRequest


@pytest.mark.asyncio
async def test_bounded_concurrency_jobs_queue_not_dropped(manager):
    # max_concurrency=2：前两个立即 RUNNING，后续排队，最终全部成功。
    ids = []
    for _ in range(5):
        job, _ = await manager.submit(
            JobSubmitRequest(
                job_type="sleep", payload={"seconds": 0.12}, run_timeout=2.0
            )
        )
        ids.append(job.id)

    views = []
    for jid in ids:
        views.append(await wait_for(manager, jid, {JobState.SUCCEEDED}, timeout=5))
    assert all(v["state"] == "SUCCEEDED" for v in views)
    assert len({v["id"] for v in views}) == 5  # 每个作业只执行一次，无丢失


@pytest.mark.asyncio
async def test_queue_full_is_explicitly_rejected(tmp_path):
    db = str(tmp_path / "q.db")
    settings = Settings(
        db_path=db, max_concurrency=1, max_queue_size=1, queue_timeout=30,
        run_timeout=10, max_attempts=1, retry_base_delay=0.01,
        retry_max_delay=0.01, poll_interval=0.01,
    )
    from app.storage import SqliteJobStorage

    storage = SqliteJobStorage(db)
    mgr = JobManager(storage, default_handlers(), settings)
    await mgr.start()
    try:
        # 占用唯一执行槽
        await mgr.submit(JobSubmitRequest(job_type="sleep", payload={"seconds": 2}))
        await asyncio.sleep(0.08)
        # 占满唯一排队槽
        await mgr.submit(JobSubmitRequest(job_type="sleep", payload={"seconds": 0.1}))
        # 第三个：队列满，明确拒绝而不是静默丢弃
        with pytest.raises(QueueFull) as exc:
            await mgr.submit(JobSubmitRequest(job_type="echo", payload={}))
        assert exc.value.error_code is ErrorCode.QUEUE_FULL
        assert exc.value.http_status == 429
    finally:
        await mgr.shutdown()
        storage.close()


@pytest.mark.asyncio
async def test_cancel_queued_job(manager):
    # 占满 2 个并发槽
    blockers = []
    for _ in range(2):
        b, _ = await manager.submit(
            JobSubmitRequest(job_type="sleep", payload={"seconds": 1.0}, run_timeout=5)
        )
        blockers.append(b.id)
    await asyncio.sleep(0.1)

    queued, _ = await manager.submit(JobSubmitRequest(job_type="echo", payload={}))
    view0 = (await manager.get(queued.id)).to_public_dict()
    assert view0["state"] in ("PENDING", "RETRY_WAIT")

    cancelled = await manager.cancel(queued.id)
    assert cancelled.state is JobState.CANCELLED

    # 终态后重复取消必须被明确拒绝
    with pytest.raises(AlreadyTerminal) as exc:
        await manager.cancel(queued.id)
    assert exc.value.error_code is ErrorCode.ALREADY_TERMINAL
    assert exc.value.details["state"] == "CANCELLED"


@pytest.mark.asyncio
async def test_cancel_running_job(manager):
    job, _ = await manager.submit(
        JobSubmitRequest(job_type="sleep", payload={"seconds": 5.0}, run_timeout=10)
    )
    # 等待进入 RUNNING
    for _ in range(100):
        v = (await manager.get(job.id)).to_public_dict()
        if v["state"] == "RUNNING":
            break
        await asyncio.sleep(0.01)
    assert v["state"] == "RUNNING"

    cancelled = await manager.cancel(job.id)
    assert cancelled.state is JobState.CANCELLED
    final = (await manager.get(job.id)).to_public_dict()
    assert final["state"] == "CANCELLED"
    assert final["terminal"] is True


@pytest.mark.asyncio
async def test_cancel_terminal_succeeded_rejected(manager):
    job, _ = await manager.submit(JobSubmitRequest(job_type="echo", payload={}))
    await wait_for(manager, job.id, {JobState.SUCCEEDED})
    with pytest.raises(AlreadyTerminal) as exc:
        await manager.cancel(job.id)
    assert exc.value.error_code is ErrorCode.ALREADY_TERMINAL
    assert exc.value.details["state"] == "SUCCEEDED"


@pytest.mark.asyncio
async def test_no_state_regression_after_cancel(manager):
    # 取消后给 worker 足够时间，状态不得回退到 RUNNING/PENDING 或变成功。
    job, _ = await manager.submit(
        JobSubmitRequest(job_type="sleep", payload={"seconds": 3.0}, run_timeout=10)
    )
    await asyncio.sleep(0.1)
    await manager.cancel(job.id)
    await asyncio.sleep(0.3)
    v = (await manager.get(job.id)).to_public_dict()
    assert v["state"] == "CANCELLED"
