"""分级调度与类型隔离：优先级、类型额度、等待提升、排队原因。

通过可控的 gate 阻塞 handler 占据 worker，精确构造排队场景，
再断言领取顺序、排队原因与额度调整行为。
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest
import pytest_asyncio
from conftest import wait_for

from app.config import Settings
from app.handlers import RetryableJobError
from app.manager import JobManager
from app.models import PRIORITY_RANK, JobState, JobSubmitRequest, Priority, QueueReason
from app.storage import SqliteJobStorage


class Harness:
    """可控执行环境：记录执行顺序、阻塞中的作业由 gate 统一放行。"""

    def __init__(self) -> None:
        self.order: list[str] = []
        self.gate = asyncio.Event()
        self.started: dict[str, asyncio.Event] = {}

    def _mark(self, tag: str) -> None:
        self.started.setdefault(tag, asyncio.Event()).set()

    def handlers(self) -> dict[str, Callable]:
        async def blocker(payload: dict, ctx) -> dict:
            tag = payload.get("tag", "blocker")
            self._mark(tag)
            await self.gate.wait()
            self.order.append(tag)
            return {"tag": tag}

        async def rec(payload: dict, ctx) -> dict:
            tag = payload.get("tag", "rec")
            self._mark(tag)
            self.order.append(tag)
            return {"tag": tag}

        async def flaky(payload: dict, ctx) -> dict:
            if ctx.attempt == 1:
                raise RetryableJobError("首次尝试确定性失败")
            return {"tag": payload.get("tag", "flaky")}

        # batch/interactive 用于类型隔离场景，行为分别同 blocker/rec。
        return {
            "blocker": blocker,
            "rec": rec,
            "flaky": flaky,
            "batch": blocker,
            "interactive": rec,
        }


@pytest_asyncio.fixture()
async def make_runtime(tmp_path) -> AsyncIterator[Callable[..., Awaitable[tuple]]]:
    """运行时工厂：每个用例按需定制并发度、类型额度与 aging 阈值。"""
    runtimes: list[tuple[JobManager, Harness, SqliteJobStorage]] = []
    counter = 0

    async def factory(
        *,
        max_concurrency: int = 1,
        type_quotas: dict[str, int] | None = None,
        aging_after: float = 30.0,
        queue_timeout: float = 30.0,
        run_timeout: float = 5.0,
        retry_base_delay: float = 0.03,
        max_attempts: int = 1,
    ) -> tuple[JobManager, Harness]:
        nonlocal counter
        counter += 1
        harness = Harness()
        settings = Settings(
            db_path=str(tmp_path / f"sched{counter}.db"),
            max_concurrency=max_concurrency,
            max_queue_size=100,
            queue_timeout=queue_timeout,
            run_timeout=run_timeout,
            max_attempts=max_attempts,
            retry_base_delay=retry_base_delay,
            retry_max_delay=1.0,
            poll_interval=0.01,
            aging_after_seconds=aging_after,
            type_quotas=dict(type_quotas or {}),
        )
        storage = SqliteJobStorage(settings.db_path)
        mgr = JobManager(storage, harness.handlers(), settings)
        await mgr.start()
        runtimes.append((mgr, harness, storage))
        return mgr, harness

    yield factory
    for mgr, harness, storage in runtimes:
        harness.gate.set()  # 放行所有阻塞作业，避免悬挂
        await mgr.shutdown()
        storage.close()


async def _submit(
    mgr: JobManager,
    job_type: str = "rec",
    tag: str | None = None,
    priority: str | None = None,
    **kwargs,
):
    req = JobSubmitRequest(
        job_type=job_type,
        payload={"tag": tag} if tag else {},
        priority=Priority(priority) if priority else None,
        **kwargs,
    )
    job, _ = await mgr.submit(req)
    return job


@pytest.mark.asyncio
async def test_priority_high_first_same_type(make_runtime):
    # 同一类型内：高优先级先被领取，低优先级最后。
    mgr, h = await make_runtime(max_concurrency=1)
    blocker = await _submit(mgr, "blocker", "blocker")
    await wait_for(mgr, blocker.id, {JobState.RUNNING})

    low = await _submit(mgr, "rec", "low", priority="low")
    normal = await _submit(mgr, "rec", "normal")
    high = await _submit(mgr, "rec", "high", priority="high")

    h.gate.set()
    for job in (low, normal, high):
        await wait_for(mgr, job.id, {JobState.SUCCEEDED})
    assert h.order == ["blocker", "high", "normal", "low"]


@pytest.mark.asyncio
async def test_same_priority_fifo(make_runtime):
    # 同一优先级档内保持先提交先执行。
    mgr, h = await make_runtime(max_concurrency=1)
    blocker = await _submit(mgr, "blocker", "blocker")
    await wait_for(mgr, blocker.id, {JobState.RUNNING})

    jobs = [await _submit(mgr, "rec", tag) for tag in ("a", "b", "c")]
    h.gate.set()
    for job in jobs:
        await wait_for(mgr, job.id, {JobState.SUCCEEDED})
    assert h.order == ["blocker", "a", "b", "c"]


@pytest.mark.asyncio
async def test_default_priority_when_omitted(make_runtime):
    # 老请求不带优先级：按 NORMAL 处理，行为不变。
    mgr, _h = await make_runtime(max_concurrency=1)
    job = await _submit(mgr, "rec", "plain")
    assert job.priority == PRIORITY_RANK[Priority.NORMAL]
    view = await wait_for(mgr, job.id, {JobState.SUCCEEDED})
    assert view["priority"] == "normal"
    assert view["queue_reason"] is None  # 终态不再有排队原因


@pytest.mark.asyncio
async def test_type_quota_isolation(make_runtime):
    # batch 额度=1：占满后不连累 interactive；batch-2 因类型额度等待。
    mgr, h = await make_runtime(max_concurrency=2, type_quotas={"batch": 1})
    b1 = await _submit(mgr, "batch", "batch-1")
    await wait_for(mgr, b1.id, {JobState.RUNNING})

    b2 = await _submit(mgr, "batch", "batch-2")
    urgent = await _submit(mgr, "interactive", "urgent")

    # 其它类型不受 batch 额度占满的影响，立即执行完成。
    await wait_for(mgr, urgent.id, {JobState.SUCCEEDED})

    # batch-2 仍在排队，原因是所属类型额度已满。
    view = (await mgr.get(b2.id)).to_public_dict()
    assert view["state"] == "PENDING"
    assert view["queue_reason"] == QueueReason.TYPE_QUOTA_FULL.value
    assert "batch-2" not in h.order

    h.gate.set()
    await wait_for(mgr, b1.id, {JobState.SUCCEEDED})
    await wait_for(mgr, b2.id, {JobState.SUCCEEDED})
    assert h.order == ["urgent", "batch-1", "batch-2"]


@pytest.mark.asyncio
async def test_global_concurrency_remains_hard_cap(make_runtime):
    # 类型额度之和(5)超过全局上限(1)时，以全局上限为准。
    mgr, h = await make_runtime(max_concurrency=1, type_quotas={"rec": 5})
    blocker = await _submit(mgr, "blocker", "blocker")
    await wait_for(mgr, blocker.id, {JobState.RUNNING})

    r1 = await _submit(mgr, "rec", "r1")
    r2 = await _submit(mgr, "rec", "r2")
    await asyncio.sleep(0.1)
    # 类型额度充足，但全局并发已被占满，两个作业都只能排队。
    assert (await mgr.get(r1.id)).state == JobState.PENDING
    assert (await mgr.get(r2.id)).state == JobState.PENDING

    h.gate.set()
    for job in (r1, r2):
        await wait_for(mgr, job.id, {JobState.SUCCEEDED})
    assert h.order == ["blocker", "r1", "r2"]


@pytest.mark.asyncio
async def test_quota_runtime_adjustment(make_runtime):
    # 额度为 0 暂停领取；运行时调大后立即生效，无需重启。
    mgr, _h = await make_runtime(max_concurrency=2, type_quotas={"rec": 0})
    paused = await _submit(mgr, "rec", "paused")
    await asyncio.sleep(0.1)
    view = (await mgr.get(paused.id)).to_public_dict()
    assert view["state"] == "PENDING"
    assert view["queue_reason"] == QueueReason.TYPE_QUOTA_FULL.value

    cfg = mgr.set_type_quotas({"rec": 1})
    assert cfg["type_quotas"] == {"rec": 1}
    await wait_for(mgr, paused.id, {JobState.SUCCEEDED})

    # 传 None 移除该类型的额度限制。
    cfg = mgr.set_type_quotas({"rec": None})
    assert cfg["type_quotas"] == {}


@pytest.mark.asyncio
async def test_quota_lowered_does_not_interrupt_running(make_runtime):
    # 调低额度只影响新领取，已在执行的作业不被打断。
    mgr, h = await make_runtime(max_concurrency=1, type_quotas={"blocker": 1})
    running = await _submit(mgr, "blocker", "running")
    await wait_for(mgr, running.id, {JobState.RUNNING})

    mgr.set_type_quotas({"blocker": 0})  # 暂停该类型新领取
    queued = await _submit(mgr, "blocker", "queued")
    await asyncio.sleep(0.1)
    assert (await mgr.get(queued.id)).state == JobState.PENDING
    assert (await mgr.get(running.id)).state == JobState.RUNNING

    h.gate.set()
    await wait_for(mgr, running.id, {JobState.SUCCEEDED})
    # 额度仍为 0：queued 继续等待；放开后才被领取。
    await asyncio.sleep(0.1)
    assert (await mgr.get(queued.id)).state == JobState.PENDING
    mgr.set_type_quotas({"blocker": 1})
    await wait_for(mgr, queued.id, {JobState.SUCCEEDED})
    assert h.order == ["running", "queued"]


@pytest.mark.asyncio
async def test_cancel_frees_type_quota(make_runtime):
    # 取消占用额度的作业后，空出的名额及时让给同类型等待作业。
    mgr, h = await make_runtime(max_concurrency=2, type_quotas={"batch": 1})
    b1 = await _submit(mgr, "batch", "batch-1")
    await wait_for(mgr, b1.id, {JobState.RUNNING})
    b2 = await _submit(mgr, "batch", "batch-2")
    await asyncio.sleep(0.1)
    assert (await mgr.get(b2.id)).state == JobState.PENDING

    await mgr.cancel(b1.id)
    await wait_for(mgr, b2.id, {JobState.RUNNING})
    h.gate.set()
    await wait_for(mgr, b2.id, {JobState.SUCCEEDED})


@pytest.mark.asyncio
async def test_aging_boost_prevents_starvation(make_runtime):
    # 等待超过 aging 阈值的低优先级作业被提升：与新高优先级同档，按 FIFO 先执行。
    mgr, h = await make_runtime(max_concurrency=1, aging_after=0.2)
    blocker = await _submit(mgr, "blocker", "blocker")
    await wait_for(mgr, blocker.id, {JobState.RUNNING})

    old_low = await _submit(mgr, "rec", "old-low", priority="low")
    await asyncio.sleep(0.3)  # 等待超过 aging_after=0.2s
    new_high = await _submit(mgr, "rec", "new-high", priority="high")

    h.gate.set()
    for job in (old_low, new_high):
        await wait_for(mgr, job.id, {JobState.SUCCEEDED})
    # 若无等待提升，new-high 会先执行；提升后 old-low 按 FIFO 排在前面。
    assert h.order == ["blocker", "old-low", "new-high"]


@pytest.mark.asyncio
async def test_queue_reason_waiting_higher_priority(make_runtime):
    # 排队原因可区分：因低优先级在等待 vs 正常等全局空位。
    mgr, h = await make_runtime(max_concurrency=1)
    blocker = await _submit(mgr, "blocker", "blocker")
    await wait_for(mgr, blocker.id, {JobState.RUNNING})

    low = await _submit(mgr, "rec", "low", priority="low")
    high = await _submit(mgr, "rec", "high", priority="high")

    view_low = (await mgr.get(low.id)).to_public_dict()
    assert view_low["queue_reason"] == QueueReason.WAITING_HIGHER_PRIORITY.value
    view_high = (await mgr.get(high.id)).to_public_dict()
    assert view_high["queue_reason"] == QueueReason.QUEUED.value

    h.gate.set()
    for job in (low, high):
        await wait_for(mgr, job.id, {JobState.SUCCEEDED})


@pytest.mark.asyncio
async def test_queue_reason_retry_backoff(make_runtime):
    # 重试退避中的作业，排队原因为 retry_backoff，退避结束后正常重试。
    mgr, _h = await make_runtime(
        max_concurrency=1, max_attempts=2, retry_base_delay=0.5
    )
    job = await _submit(mgr, "flaky", "flaky-job")
    view = await wait_for(mgr, job.id, {JobState.RETRY_WAIT})
    assert view["queue_reason"] == QueueReason.RETRY_BACKOFF.value
    await wait_for(mgr, job.id, {JobState.SUCCEEDED})


@pytest.mark.asyncio
async def test_idempotency_unaffected_by_priority(make_runtime):
    # 幂等判定不看优先级：同键同载荷去重命中，返回既有作业。
    mgr, _h = await make_runtime(max_concurrency=1)
    req = JobSubmitRequest(
        job_type="rec", payload={"tag": "idem"},
        idempotency_key="k-1", priority=Priority.LOW,
    )
    first, dedup1 = await mgr.submit(req)
    req2 = JobSubmitRequest(
        job_type="rec", payload={"tag": "idem"},
        idempotency_key="k-1", priority=Priority.HIGH,
    )
    second, dedup2 = await mgr.submit(req2)
    assert not dedup1 and dedup2
    assert first.id == second.id
    await wait_for(mgr, first.id, {JobState.SUCCEEDED})
