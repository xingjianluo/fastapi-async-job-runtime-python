"""并发提交与并发取消：结论必须一致、可区分。"""

from __future__ import annotations

import asyncio

import pytest
from conftest import wait_for

from app.errors import AlreadyTerminal, ErrorCode, IdempotencyConflict
from app.models import JobState, JobSubmitRequest


@pytest.mark.asyncio
async def test_concurrent_submit_same_idempotency_key(manager):
    # 20 个协程用同一幂等键并发提交：恰好一个作业，其余全部去重命中。
    n = 20
    results = await asyncio.gather(*[
        manager.submit(
            JobSubmitRequest(job_type="echo", payload={"v": 1}, idempotency_key="RACE-1")
        )
        for _ in range(n)
    ])
    ids = {job.id for job, _ in results}
    assert len(ids) == 1
    created = [job for job, dedup in results if not dedup]
    deduped = [job for job, dedup in results if dedup]
    assert len(created) == 1
    assert len(deduped) == n - 1

    only_id = next(iter(ids))
    view = await wait_for(manager, only_id, {JobState.SUCCEEDED})
    assert view["state"] == "SUCCEEDED"
    assert len(view["attempts"]) == 1  # 只执行一次


@pytest.mark.asyncio
async def test_concurrent_submit_conflicting_payloads(manager):
    # 同一幂等键、两种载荷并发提交：恰好一个成功，其余要么去重(同载荷)
    # 要么得到 IDEMPOTENCY_CONFLICT（异载荷），绝不产生两个作业。
    async def submit(v):
        return await manager.submit(
            JobSubmitRequest(job_type="echo", payload={"v": v}, idempotency_key="RACE-2")
        )

    results = await asyncio.gather(
        *[submit(1) for _ in range(10)] + [submit(2) for _ in range(10)],
        return_exceptions=True,
    )
    ok_jobs, conflicts = [], 0
    for r in results:
        if isinstance(r, IdempotencyConflict):
            conflicts += 1
            assert r.error_code is ErrorCode.IDEMPOTENCY_CONFLICT
        else:
            ok_jobs.append(r[0].id)
    assert len(set(ok_jobs)) == 1
    assert conflicts >= 1


@pytest.mark.asyncio
async def test_concurrent_cancel_only_one_wins(manager):
    job, _ = await manager.submit(
        JobSubmitRequest(job_type="sleep", payload={"seconds": 2.0}, run_timeout=5)
    )
    # 等进入 RUNNING 后并发取消
    await asyncio.sleep(0.1)
    outcomes = await asyncio.gather(
        *[manager.cancel(job.id) for _ in range(8)],
        return_exceptions=True,
    )
    cancelled = [o for o in outcomes if not isinstance(o, Exception)]
    rejected = [o for o in outcomes if isinstance(o, AlreadyTerminal)]
    assert len(cancelled) == 1
    assert cancelled[0].state is JobState.CANCELLED
    assert len(rejected) == 7
    assert all(o.error_code is ErrorCode.ALREADY_TERMINAL for o in rejected)

    final = (await manager.get(job.id)).to_public_dict()
    assert final["state"] == "CANCELLED"


@pytest.mark.asyncio
async def test_cancel_and_complete_race_is_consistent(manager):
    # 极短作业：取消与完成竞争，最终状态只能是 SUCCEEDED 或 CANCELLED 之一，
    # 且终态唯一、不回退。
    job, _ = await manager.submit(
        JobSubmitRequest(job_type="sleep", payload={"seconds": 0.05}, run_timeout=2)
    )
    await asyncio.sleep(0.01)
    cancel_result = await asyncio.gather(manager.cancel(job.id), return_exceptions=True)
    view = await wait_for(
        manager, job.id, {JobState.SUCCEEDED, JobState.CANCELLED}, timeout=3
    )
    assert view["state"] in ("SUCCEEDED", "CANCELLED")
    assert view["terminal"] is True
    # 若取消抛 AlreadyTerminal，说明作业先成功；二者必居其一且自洽。
    if isinstance(cancel_result[0], AlreadyTerminal):
        assert view["state"] == "SUCCEEDED"
    else:
        assert view["state"] == "CANCELLED"
