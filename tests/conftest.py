"""pytest 公共夹具：快速配置的运行时、直接管理器与 HTTP 客户端。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.config import Settings
from app.handlers import default_handlers
from app.manager import JobManager
from app.models import JobState
from app.storage import SqliteJobStorage
from main import create_app


def fast_settings(db_path: str) -> Settings:
    """测试用确定配置：小并发、短超时、短退避。"""
    return Settings(
        db_path=db_path,
        max_concurrency=2,
        max_queue_size=10,
        queue_timeout=1.0,
        run_timeout=0.3,
        max_attempts=3,
        retry_base_delay=0.03,
        retry_max_delay=0.08,
        poll_interval=0.01,
    )


@pytest_asyncio.fixture()
async def manager(tmp_path) -> AsyncIterator[JobManager]:
    db = str(tmp_path / "jobs.db")
    storage = SqliteJobStorage(db)
    mgr = JobManager(storage, default_handlers(), fast_settings(db))
    await mgr.start()
    try:
        yield mgr
    finally:
        await mgr.shutdown()
        storage.close()


@pytest_asyncio.fixture()
async def client(tmp_path) -> AsyncIterator[AsyncClient]:
    db = str(tmp_path / "jobs.db")
    app = create_app(fast_settings(db))
    transport = ASGITransport(app=app)
    # 先进入 lifespan（启动运行时），再创建客户端；退出时逆序关闭。
    async with app.router.lifespan_context(app), AsyncClient(
        transport=transport, base_url="http://test"
    ) as ac:
        yield ac


async def wait_for(
    mgr_or_client, job_id, expect: set[JobState], *, timeout: float = 5.0,
    interval: float = 0.01, via_http: bool = False,
) -> dict:
    """轮询直到作业进入期望状态集合，返回最终作业视图（dict）。"""
    deadline = asyncio.get_running_loop().time() + timeout
    last = None
    while asyncio.get_running_loop().time() < deadline:
        if via_http:
            resp = await mgr_or_client.get(f"/jobs/{job_id}")
            last = resp.json()
        else:
            last = (await mgr_or_client.get(job_id)).to_public_dict()
        if JobState(last["state"]) in expect:
            return last
        await asyncio.sleep(interval)
    raise AssertionError(f"作业 {job_id} 未进入 {expect}，最终状态 {last and last['state']}")
