"""分级调度与类型隔离：优先级、类型额度、运行中调整、等待提升、排队原因。

每条用例通过 logging 输出作业标识、状态变化与调度原因，配合
`pytest -v --log-cli-level=INFO` 可看到完整调度轨迹与逐条通过情况。
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from conftest import wait_for

from app.config import Settings
from app.handlers import sleep_handler
from app.manager import JobManager
from app.models import JobState, JobSubmitRequest, Priority, QueueReason
from app.storage import SqliteJobStorage

logger = logging.getLogger("test_scheduling")


def _settings(db: str, **over) -> Settings:
    kw = {
        "db_path": db,
        "max_concurrency": 1,
        "max_queue_size": 50,
        "queue_timeout": 5.0,
        "run_timeout": 5.0,
        "max_attempts": 1,
        "retry_base_delay": 0.03,
        "retry_max_delay": 0.08,
        "poll_interval": 0.01,
        "type_quotas": {},
        "aging_threshold": 60.0,  # 默认关闭等待提升，按需覆盖
        "aging_max_boost": 2,
    }
    kw.update(over)
    return Settings(**kw)


@asynccontextmanager
async def runtime(tmp_path, name: str, handlers: dict, **over) -> AsyncIterator[JobManager]:
    db = str(tmp_path / f"{name}.db")
    storage = SqliteJobStorage(db)
    mgr = JobManager(storage, handlers, _settings(db, **over))
    await mgr.start()
    try:
        yield mgr
    finally:
        await mgr.shutdown()
        storage.close()


def rec_handler(order: list[str]):
    """领取即记录 job_id，用于断言调度顺序。"""

    async def _h(payload, ctx):
        order.append(ctx.job_id)
        await asyncio.sleep(float(payload.get("seconds", 0.01)))
        return {"job_id": ctx.job_id}

    return _h


class ConcurrencyTracker:
    """记录某类型处理器的并发峰值。"""

    def __init__(self) -> None:
        self.cur = 0
        self.peak = 0

    async def __call__(self, payload, ctx):
        self.cur += 1
        self.peak = max(self.peak, self.cur)
        try:
            await asyncio.sleep(float(payload.get("seconds", 0.1)))
            return {"job_id": ctx.job_id}
        finally:
            self.cur -= 1


def view_state(v: dict) -> str:
    return f"job={v['id']} state={v['state']} priority={v['priority']} queue_reason={v['queue_reason']}"


@pytest.mark.asyncio
async def test_high_priority_claimed_first_same_type(tmp_path):
    """同类型内高优先级先被领取（全局单并发下观察）。"""
    order: list[str] = []
    handlers = {"rec": rec_handler(order), "sleep": sleep_handler}
    async with runtime(tmp_path, "prio", handlers, max_concurrency=1) as mgr:
        blocker, _ = await mgr.submit(
            JobSubmitRequest(job_type="sleep", payload={"seconds": 0.3}, run_timeout=5)
        )
        await wait_for(mgr, blocker.id, {JobState.RUNNING})
        low, _ = await mgr.submit(
            JobSubmitRequest(job_type="rec", payload={}, priority=Priority.LOW)
        )
        high, _ = await mgr.submit(
            JobSubmitRequest(job_type="rec", payload={}, priority=Priority.HIGH)
        )
        logger.info("提交 low=%s priority=1, high=%s priority=3（low 先提交）", low.id, high.id)
        await wait_for(mgr, high.id, {JobState.SUCCEEDED})
        await wait_for(mgr, low.id, {JobState.SUCCEEDED})
        logger.info("领取顺序=%s，期望 high 在 low 前", order)
        assert order == [high.id, low.id]
    logger.info("用例通过：test_high_priority_claimed_first_same_type")


@pytest.mark.asyncio
async def test_same_priority_keeps_fifo(tmp_path):
    """同档优先级保持先提交先执行。"""
    order: list[str] = []
    handlers = {"rec": rec_handler(order), "sleep": sleep_handler}
    async with runtime(tmp_path, "fifo", handlers, max_concurrency=1) as mgr:
        blocker, _ = await mgr.submit(
            JobSubmitRequest(job_type="sleep", payload={"seconds": 0.3}, run_timeout=5)
        )
        await wait_for(mgr, blocker.id, {JobState.RUNNING})
        jobs = [
            (await mgr.submit(JobSubmitRequest(job_type="rec", payload={"i": i})))[0]
            for i in range(3)
        ]
        for j in jobs:
            await wait_for(mgr, j.id, {JobState.SUCCEEDED})
        logger.info("领取顺序=%s，期望与提交顺序一致", order)
        assert order == [j.id for j in jobs]
    logger.info("用例通过：test_same_priority_keeps_fifo")


@pytest.mark.asyncio
async def test_default_priority_backward_compatible(tmp_path):
    """老请求不带优先级：默认 NORMAL，幂等去重判定不受优先级影响。"""
    order: list[str] = []
    handlers = {"rec": rec_handler(order)}
    async with runtime(tmp_path, "compat", handlers, max_concurrency=1) as mgr:
        job, dedup = await mgr.submit(
            JobSubmitRequest(job_type="rec", payload={"a": 1}, idempotency_key="k1")
        )
        assert dedup is False
        assert job.priority == Priority.NORMAL
        view = (await mgr.get(job.id)).to_public_dict()
        logger.info("老请求缺省优先级：%s", view_state(view))
        assert view["priority"] == 2
        assert view["priority_name"] == "NORMAL"

        # 同一幂等键 + 相同载荷，即使带了不同优先级也按既有规则去重
        again, dedup2 = await mgr.submit(
            JobSubmitRequest(
                job_type="rec", payload={"a": 1},
                idempotency_key="k1", priority=Priority.HIGH,
            )
        )
        assert dedup2 is True
        assert again.id == job.id
        assert again.priority == Priority.NORMAL  # 保留首次提交的值
        await wait_for(mgr, job.id, {JobState.SUCCEEDED})
    logger.info("用例通过：test_default_priority_backward_compatible")


@pytest.mark.asyncio
async def test_type_quota_isolation_and_reason(tmp_path):
    """类型额度隔离：batch 额度占满不连累 rec；等待作业报告 TYPE_QUOTA_FULL。"""
    order: list[str] = []
    handlers = {"batch": sleep_handler, "rec": rec_handler(order)}
    async with runtime(
        tmp_path, "quota", handlers, max_concurrency=2, type_quotas={"batch": 1}
    ) as mgr:
        b1, _ = await mgr.submit(
            JobSubmitRequest(job_type="batch", payload={"seconds": 0.4}, run_timeout=5)
        )
        await wait_for(mgr, b1.id, {JobState.RUNNING})
        b2, _ = await mgr.submit(
            JobSubmitRequest(job_type="batch", payload={"seconds": 0.1}, run_timeout=5)
        )
        r1, _ = await mgr.submit(JobSubmitRequest(job_type="rec", payload={}))

        # rec 类型不受 batch 额度影响，直接被执行
        v_r1 = await wait_for(mgr, r1.id, {JobState.SUCCEEDED})
        logger.info("rec 作业未被 batch 占满连累：%s", view_state(v_r1))

        # b2 因所属类型额度满而等待，原因可解释
        v_b2 = (await mgr.get(b2.id)).to_public_dict()
        logger.info("batch 等待作业：%s", view_state(v_b2))
        assert v_b2["state"] == "PENDING"
        assert v_b2["queue_reason"] == QueueReason.TYPE_QUOTA_FULL.value

        v_b1 = await wait_for(mgr, b1.id, {JobState.SUCCEEDED})
        v_b2 = await wait_for(mgr, b2.id, {JobState.SUCCEEDED})
        # 额度为 1：b2 必须在 b1 结束后才开始
        assert v_b1["finished_at"] <= v_b2["started_at"]
    logger.info("用例通过：test_type_quota_isolation_and_reason")


@pytest.mark.asyncio
async def test_global_cap_is_hard_limit(tmp_path):
    """各类型额度之和超过全局上限时，以全局上限为准。"""
    tracker = ConcurrencyTracker()
    handlers = {"a": tracker, "b": tracker}
    async with runtime(
        tmp_path, "global", handlers,
        max_concurrency=2, type_quotas={"a": 5, "b": 5},
    ) as mgr:
        jobs = []
        for i in range(4):
            j, _ = await mgr.submit(
                JobSubmitRequest(
                    job_type="a" if i % 2 == 0 else "b",
                    payload={"seconds": 0.15},
                )
            )
            jobs.append(j)
        for j in jobs:
            await wait_for(mgr, j.id, {JobState.SUCCEEDED})
        logger.info("额度之和 10 > 全局上限 2，实际并发峰值=%d", tracker.peak)
        assert tracker.peak == 2
    logger.info("用例通过：test_global_cap_is_hard_limit")


@pytest.mark.asyncio
async def test_runtime_quota_adjustment_no_restart(tmp_path):
    """不重启调整额度：只影响新领取，已在执行的不被打断。"""
    handlers = {"batch": sleep_handler}
    async with runtime(
        tmp_path, "adjust", handlers, max_concurrency=2, type_quotas={"batch": 1}
    ) as mgr:
        b1, _ = await mgr.submit(
            JobSubmitRequest(job_type="batch", payload={"seconds": 0.4}, run_timeout=5)
        )
        await wait_for(mgr, b1.id, {JobState.RUNNING})
        b2, _ = await mgr.submit(
            JobSubmitRequest(job_type="batch", payload={"seconds": 0.2}, run_timeout=5)
        )
        v_b2 = (await mgr.get(b2.id)).to_public_dict()
        assert v_b2["queue_reason"] == QueueReason.TYPE_QUOTA_FULL.value
        logger.info("调整前 b2 等待：%s", view_state(v_b2))

        # 运行中调大批量额度，b2 应立即被领取，b1 继续执行不中断
        effective = mgr.set_type_quotas({"batch": 2})
        assert effective == {"batch": 2}
        logger.info("运行中调整额度 quotas=%s", effective)
        v_b2 = await wait_for(mgr, b2.id, {JobState.SUCCEEDED})
        v_b1 = await wait_for(mgr, b1.id, {JobState.SUCCEEDED})
        # b2 在 b1 结束前就开始执行：调整生效且未打断 b1
        assert v_b2["started_at"] < v_b1["finished_at"]
        assert v_b1["state"] == "SUCCEEDED"
    logger.info("用例通过：test_runtime_quota_adjustment_no_restart")


@pytest.mark.asyncio
async def test_cancel_frees_type_quota(tmp_path):
    """取消执行中的作业后，空出的类型名额及时让给等待中的同类作业。"""
    handlers = {"batch": sleep_handler}
    async with runtime(
        tmp_path, "cancelfree", handlers,
        max_concurrency=2, type_quotas={"batch": 1},
    ) as mgr:
        b1, _ = await mgr.submit(
            JobSubmitRequest(job_type="batch", payload={"seconds": 5.0}, run_timeout=10)
        )
        await wait_for(mgr, b1.id, {JobState.RUNNING})
        b2, _ = await mgr.submit(
            JobSubmitRequest(job_type="batch", payload={"seconds": 0.05}, run_timeout=5)
        )
        await mgr.cancel(b1.id)
        logger.info("已取消 b1=%s，等待 b2=%s 接过名额", b1.id, b2.id)
        v_b2 = await wait_for(mgr, b2.id, {JobState.SUCCEEDED}, timeout=3)
        assert v_b2["state"] == "SUCCEEDED"
        v_b1 = (await mgr.get(b1.id)).to_public_dict()
        assert v_b1["state"] == "CANCELLED"
    logger.info("用例通过：test_cancel_frees_type_quota")


@pytest.mark.asyncio
async def test_aging_boost_prevents_starvation(tmp_path):
    """等待超过阈值的低优先级作业被确定性地提升，不会被饿死。"""
    order: list[str] = []
    handlers = {"rec": rec_handler(order), "sleep": sleep_handler}
    async with runtime(
        tmp_path, "aging", handlers,
        max_concurrency=1, aging_threshold=0.1, aging_max_boost=2,
    ) as mgr:
        blocker, _ = await mgr.submit(
            JobSubmitRequest(job_type="sleep", payload={"seconds": 0.35}, run_timeout=5)
        )
        await wait_for(mgr, blocker.id, {JobState.RUNNING})
        low, _ = await mgr.submit(
            JobSubmitRequest(job_type="rec", payload={}, priority=Priority.LOW)
        )
        await asyncio.sleep(0.25)  # low 等待超过 2 个阈值：有效优先级提升 2 档
        normal, _ = await mgr.submit(
            JobSubmitRequest(job_type="rec", payload={}, priority=Priority.NORMAL)
        )
        logger.info(
            "low=%s 已等待 0.25s（threshold=0.1，提升 2 档），normal=%s 刚提交",
            low.id, normal.id,
        )
        await wait_for(mgr, low.id, {JobState.SUCCEEDED})
        await wait_for(mgr, normal.id, {JobState.SUCCEEDED})
        logger.info("领取顺序=%s，期望被提升的 low 在 normal 前", order)
        assert order == [low.id, normal.id]
    logger.info("用例通过：test_aging_boost_prevents_starvation")


@pytest.mark.asyncio
async def test_queue_reason_priority_wait(tmp_path):
    """优先级低在等：可区分 PRIORITY_WAIT 与普通 QUEUED。"""
    order: list[str] = []
    handlers = {"rec": rec_handler(order), "sleep": sleep_handler}
    async with runtime(tmp_path, "reason", handlers, max_concurrency=1) as mgr:
        blocker, _ = await mgr.submit(
            JobSubmitRequest(job_type="sleep", payload={"seconds": 0.4}, run_timeout=5)
        )
        await wait_for(mgr, blocker.id, {JobState.RUNNING})
        high, _ = await mgr.submit(
            JobSubmitRequest(job_type="rec", payload={}, priority=Priority.HIGH)
        )
        low, _ = await mgr.submit(
            JobSubmitRequest(job_type="rec", payload={}, priority=Priority.LOW)
        )
        v_low = (await mgr.get(low.id)).to_public_dict()
        v_high = (await mgr.get(high.id)).to_public_dict()
        logger.info("低优先级等待：%s", view_state(v_low))
        logger.info("高优先级等待：%s", view_state(v_high))
        assert v_low["queue_reason"] == QueueReason.PRIORITY_WAIT.value
        assert v_high["queue_reason"] == QueueReason.QUEUED.value
        await wait_for(mgr, high.id, {JobState.SUCCEEDED})
        await wait_for(mgr, low.id, {JobState.SUCCEEDED})
        assert order == [high.id, low.id]
    logger.info("用例通过：test_queue_reason_priority_wait")


@pytest.mark.asyncio
async def test_queue_reason_retry_backoff(tmp_path):
    """失败重试退避中的作业报告 RETRY_BACKOFF，且退避未满不被领取。"""
    handlers = {"flaky": None}  # 占位，下面替换

    async def flaky(payload, ctx):
        if ctx.attempt == 1:
            from app.handlers import RetryableJobError

            raise RetryableJobError("首次必失败", error_type="Simulated")
        return {"ok": True}

    handlers["flaky"] = flaky
    async with runtime(
        tmp_path, "backoff", handlers,
        max_concurrency=2, retry_base_delay=0.4, retry_max_delay=0.4,
    ) as mgr:
        job, _ = await mgr.submit(
            JobSubmitRequest(job_type="flaky", payload={}, max_attempts=2)
        )
        v = await wait_for(mgr, job.id, {JobState.RETRY_WAIT})
        logger.info("退避中：%s", view_state(v))
        assert v["queue_reason"] == QueueReason.RETRY_BACKOFF.value
        # 退避期间即使有空闲额度也不得被领取
        await asyncio.sleep(0.1)
        v2 = (await mgr.get(job.id)).to_public_dict()
        assert v2["state"] == "RETRY_WAIT"
        v_done = await wait_for(mgr, job.id, {JobState.SUCCEEDED})
        assert v_done["attempt"] == 2
    logger.info("用例通过：test_queue_reason_retry_backoff")


@pytest.mark.asyncio
async def test_queue_timeout_still_applies_when_quota_blocked(tmp_path):
    """被类型额度挡住时，排队超时规则仍然生效（进入终态 TIMED_OUT）。"""
    handlers = {"batch": sleep_handler}
    async with runtime(
        tmp_path, "qtimeout", handlers,
        max_concurrency=1, type_quotas={"batch": 1},
    ) as mgr:
        blocker, _ = await mgr.submit(
            JobSubmitRequest(job_type="batch", payload={"seconds": 0.6}, run_timeout=5)
        )
        await wait_for(mgr, blocker.id, {JobState.RUNNING})
        waiting, _ = await mgr.submit(
            JobSubmitRequest(
                job_type="batch", payload={"seconds": 0.05},
                run_timeout=5, queue_timeout=0.2,
            )
        )
        v = await wait_for(mgr, waiting.id, {JobState.TIMED_OUT})
        logger.info("额度阻塞期间排队超时：%s error_code=%s", view_state(v), v["error_code"])
        assert v["error_code"] == "QUEUE_TIMEOUT"
    logger.info("用例通过：test_queue_timeout_still_applies_when_quota_blocked")


@pytest.mark.asyncio
async def test_restart_recovery_preserves_priority(tmp_path):
    """重启恢复：RUNNING 置回 PENDING 后按原优先级重新被领取，不重复执行。"""
    db = str(tmp_path / "recover.db")
    order: list[str] = []

    async def rec(payload, ctx):
        order.append(ctx.job_id)
        return {"job_id": ctx.job_id}

    handlers = {"sleep": sleep_handler, "rec": rec}
    storage1 = SqliteJobStorage(db)
    mgr1 = JobManager(storage1, handlers, _settings(db, max_concurrency=1))
    await mgr1.start()
    running, _ = await mgr1.submit(
        JobSubmitRequest(job_type="sleep", payload={"seconds": 5.0}, run_timeout=10)
    )
    await wait_for(mgr1, running.id, {JobState.RUNNING})
    await mgr1.shutdown()  # 模拟服务停止：执行中的作业遗留 RUNNING
    storage1.close()

    storage2 = SqliteJobStorage(db)
    mgr2 = JobManager(storage2, handlers, _settings(db, max_concurrency=1))
    await mgr2.start()
    try:
        high, _ = await mgr2.submit(
            JobSubmitRequest(job_type="rec", payload={}, priority=Priority.HIGH)
        )
        v_running = await wait_for(mgr2, running.id, {JobState.SUCCEEDED})
        v_high = await wait_for(mgr2, high.id, {JobState.SUCCEEDED})
        logger.info("恢复作业：%s；新作业：%s", view_state(v_running), view_state(v_high))
        assert v_running["priority"] == Priority.NORMAL  # 恢复不丢优先级字段
        assert order == [high.id]  # rec 只执行一次，无重复领取
    finally:
        await mgr2.shutdown()
        storage2.close()
    logger.info("用例通过：test_restart_recovery_preserves_priority")


@pytest.mark.asyncio
async def test_http_quota_endpoints(client):
    """HTTP：查询/调整类型额度；非法额度被 422 拒绝；老请求不带优先级不报错。"""
    resp = await client.get("/scheduler/quotas")
    assert resp.status_code == 200
    assert resp.json() == {"quotas": {}, "max_concurrency": 2}
    logger.info("初始额度：%s", resp.json())

    resp = await client.put("/scheduler/quotas", json={"quotas": {"batch": 1}})
    assert resp.status_code == 200
    assert resp.json()["quotas"] == {"batch": 1}
    logger.info("调整后额度：%s", resp.json())

    resp = await client.put("/scheduler/quotas", json={"quotas": {"batch": -1}})
    assert resp.status_code == 422

    # 老请求：不带 priority 字段，接口不报错且按默认优先级处理
    resp = await client.post("/jobs", json={"job_type": "echo", "payload": {}})
    assert resp.status_code == 201
    job_id = resp.json()["id"]
    view = await wait_for(client, job_id, {JobState.SUCCEEDED}, via_http=True)
    logger.info("HTTP 老请求缺省优先级：%s", view_state(view))
    assert view["priority"] == 2
    assert view["queue_reason"] is None  # 终态不再有排队原因
    logger.info("用例通过：test_http_quota_endpoints")
