"""服务重启后：终态结果可查、未完成作业不丢失不重复执行。"""

from __future__ import annotations

import asyncio

import pytest
from conftest import wait_for

from app.config import Settings
from app.handlers import default_handlers
from app.manager import JobManager
from app.models import JobState, JobSubmitRequest
from app.storage import SqliteJobStorage


def _settings(db: str) -> Settings:
    return Settings(
        db_path=db, max_concurrency=1, max_queue_size=10, queue_timeout=30,
        run_timeout=2.0, max_attempts=1, retry_base_delay=0.01,
        retry_max_delay=0.01, poll_interval=0.01,
    )


@pytest.mark.asyncio
async def test_terminal_results_survive_restart(tmp_path):
    db = str(tmp_path / "p.db")
    storage = SqliteJobStorage(db)
    mgr = JobManager(storage, default_handlers(), _settings(db))
    await mgr.start()
    job, _ = await mgr.submit(
        JobSubmitRequest(job_type="echo", payload={"persisted": True})
    )
    await wait_for(mgr, job.id, {JobState.SUCCEEDED})
    await mgr.shutdown()
    storage.close()

    # 全新连接 + 全新管理器，模拟进程重启
    storage2 = SqliteJobStorage(db)
    mgr2 = JobManager(storage2, default_handlers(), _settings(db))
    await mgr2.start()
    try:
        view = await wait_for(mgr2, job.id, {JobState.SUCCEEDED})
        assert view["state"] == "SUCCEEDED"
        assert view["output"]["echoed"] == {"persisted": True}
        assert len(view["attempts"]) == 1
    finally:
        await mgr2.shutdown()
        storage2.close()


@pytest.mark.asyncio
async def test_running_job_recovered_after_restart(tmp_path):
    db = str(tmp_path / "r.db")
    storage = SqliteJobStorage(db)
    mgr = JobManager(storage, default_handlers(), _settings(db))
    await mgr.start()
    # 长作业，停服时正处于 RUNNING
    job, _ = await mgr.submit(
        JobSubmitRequest(job_type="sleep", payload={"seconds": 0.2}, run_timeout=2)
    )
    await asyncio.sleep(0.05)
    assert (await mgr.get(job.id)).state is JobState.RUNNING
    # 直接关闭，模拟进程中断（不等待作业完成）
    await mgr.shutdown()
    storage.close()

    storage2 = SqliteJobStorage(db)
    mgr2 = JobManager(storage2, default_handlers(), _settings(db))
    await mgr2.start()  # 启动时执行 RUNNING -> PENDING 恢复
    try:
        view = await wait_for(mgr2, job.id, {JobState.SUCCEEDED}, timeout=5)
        assert view["state"] == "SUCCEEDED"
    finally:
        await mgr2.shutdown()
        storage2.close()


@pytest.mark.asyncio
async def test_queued_job_runs_once_after_restart(tmp_path):
    """未开始执行（排队中）的作业在重启后只执行一次，不丢失也不重复。"""
    db = str(tmp_path / "qonce.db")
    runs = {"n": 0}

    async def counting_handler(payload, ctx):
        runs["n"] += 1
        return {"runs": runs["n"]}

    # 单并发：先占住执行槽，使目标作业停留在 PENDING（从未执行）。
    settings = Settings(
        db_path=db, max_concurrency=1, max_queue_size=10, queue_timeout=30,
        run_timeout=2.0, max_attempts=1, retry_base_delay=0.01,
        retry_max_delay=0.01, poll_interval=0.01,
    )
    storage = SqliteJobStorage(db)
    handlers = {"sleep": default_handlers()["sleep"], "count": counting_handler}
    mgr = JobManager(storage, handlers, settings)
    await mgr.start()
    blocker, _ = await mgr.submit(
        JobSubmitRequest(job_type="sleep", payload={"seconds": 1.0})
    )
    await asyncio.sleep(0.05)
    job, _ = await mgr.submit(JobSubmitRequest(job_type="count", payload={}))
    assert (await mgr.get(job.id)).state is JobState.PENDING
    await mgr.shutdown()
    storage.close()
    assert runs["n"] == 0  # 从未开始执行

    storage2 = SqliteJobStorage(db)
    mgr2 = JobManager(storage2, handlers, settings)
    await mgr2.start()
    try:
        view = await wait_for(mgr2, job.id, {JobState.SUCCEEDED}, timeout=6)
        assert view["state"] == "SUCCEEDED"
        bview = await wait_for(mgr2, blocker.id, {JobState.SUCCEEDED}, timeout=6)
        assert bview["state"] == "SUCCEEDED"
    finally:
        await mgr2.shutdown()
        storage2.close()
    assert runs["n"] == 1  # 重启后恰好执行一次


@pytest.mark.asyncio
async def test_running_job_at_least_once_after_recovery(tmp_path):
    """RUNNING 中崩溃：启动恢复为 PENDING 并有界地补跑一次（at-least-once）。"""
    db = str(tmp_path / "d.db")
    counter = {"n": 0}

    async def counting_handler(payload, ctx):
        counter["n"] += 1
        await asyncio.sleep(0.15)
        return {"runs": counter["n"]}

    storage = SqliteJobStorage(db)
    mgr = JobManager(storage, {"count": counting_handler}, _settings(db))
    await mgr.start()
    job, _ = await mgr.submit(JobSubmitRequest(job_type="count", payload={}))
    await asyncio.sleep(0.05)
    await mgr.shutdown()
    storage.close()
    assert counter["n"] == 1  # 崩溃前已开始一次

    storage2 = SqliteJobStorage(db)
    mgr2 = JobManager(storage2, {"count": counting_handler}, _settings(db))
    await mgr2.start()
    try:
        view = await wait_for(mgr2, job.id, {JobState.SUCCEEDED}, timeout=5)
        assert view["state"] == "SUCCEEDED"
    finally:
        await mgr2.shutdown()
        storage2.close()
    # 恢复后至多补跑一次（有界、不并发重复），共 <= 2 次。
    assert counter["n"] <= 2
