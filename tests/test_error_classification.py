"""失败分类在终态字段与每次尝试记录中必须一致且可区分。"""

from __future__ import annotations

import pytest
from conftest import wait_for

from app.models import JobState, JobSubmitRequest


@pytest.mark.asyncio
async def test_non_retryable_classification(manager):
    job, _ = await manager.submit(
        JobSubmitRequest(job_type="fail", payload={"message": "bad"})
    )
    v = await wait_for(manager, job.id, {JobState.FAILED})
    assert v["failure_kind"] == "NON_RETRYABLE"
    assert v["error_code"] == "NON_RETRYABLE_FAILURE"
    (att,) = v["attempts"]
    assert att["result"]["failure_kind"] == "NON_RETRYABLE"
    assert att["result"]["error_type"] == "RuntimeError"


@pytest.mark.asyncio
async def test_retryable_then_exhausted_classification(manager):
    job, _ = await manager.submit(
        JobSubmitRequest(job_type="fail", payload={"kind": "retryable"})
    )
    v = await wait_for(manager, job.id, {JobState.FAILED})
    assert v["failure_kind"] == "RETRYABLE"
    assert v["error_code"] == "RETRYABLE_FAILURE"
    assert len(v["attempts"]) == 3
    for att in v["attempts"]:
        assert att["result"]["failure_kind"] == "RETRYABLE"
        assert att["result"]["ok"] is False


@pytest.mark.asyncio
async def test_timeout_classification(manager):
    job, _ = await manager.submit(
        JobSubmitRequest(job_type="sleep", payload={"seconds": 1}, run_timeout=0.03)
    )
    v = await wait_for(manager, job.id, {JobState.TIMED_OUT})
    assert v["failure_kind"] == "TIMEOUT"
    assert v["error_code"] == "ATTEMPT_TIMEOUT"
    for att in v["attempts"]:
        assert att["result"]["failure_kind"] == "TIMEOUT"


@pytest.mark.asyncio
async def test_queue_timeout_classification(manager):
    # 占满两个 worker，再排队一个 queue_timeout 很短的作业。
    import asyncio

    for _ in range(2):
        await manager.submit(
            JobSubmitRequest(job_type="sleep", payload={"seconds": 1.0}, run_timeout=5)
        )
    await asyncio.sleep(0.08)
    job, _ = await manager.submit(
        JobSubmitRequest(job_type="echo", payload={}, queue_timeout=0.05)
    )
    v = await wait_for(manager, job.id, {JobState.TIMED_OUT})
    assert v["failure_kind"] == "TIMEOUT"
    assert v["error_code"] == "QUEUE_TIMEOUT"
    assert v["state"] == "TIMED_OUT"


@pytest.mark.asyncio
async def test_terminal_states_are_distinguishable(manager):
    succ, _ = await manager.submit(JobSubmitRequest(job_type="echo", payload={}))
    failed, _ = await manager.submit(JobSubmitRequest(job_type="fail", payload={}))
    timeout, _ = await manager.submit(
        JobSubmitRequest(job_type="sleep", payload={"seconds": 1}, run_timeout=0.02)
    )
    blocked = []
    import asyncio

    for _ in range(2):
        b, _ = await manager.submit(
            JobSubmitRequest(job_type="sleep", payload={"seconds": 1.0}, run_timeout=5)
        )
        blocked.append(b)
    await asyncio.sleep(0.08)
    cancel, _ = await manager.submit(JobSubmitRequest(job_type="echo", payload={}))
    await manager.cancel(cancel.id)

    sv = await wait_for(manager, succ.id, {JobState.SUCCEEDED})
    fv = await wait_for(manager, failed.id, {JobState.FAILED})
    tv = await wait_for(manager, timeout.id, {JobState.TIMED_OUT})
    cv = await wait_for(manager, cancel.id, {JobState.CANCELLED})
    states = {sv["state"], fv["state"], tv["state"], cv["state"]}
    assert states == {"SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED"}
    for v in (sv, fv, tv, cv):
        assert v["terminal"] is True


@pytest.mark.asyncio
async def test_http_error_bodies_are_distinguishable(client):
    # 未知类型
    r = await client.post("/jobs", json={"job_type": "nope"})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "UNKNOWN_JOB_TYPE"

    # 校验错误
    r = await client.post("/jobs", json={"job_type": ""})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "VALIDATION_ERROR"

    # 找不到作业
    r = await client.get("/jobs/nope")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "JOB_NOT_FOUND"

    # 对已成功作业取消 -> ALREADY_TERMINAL
    r = await client.post("/jobs", json={"job_type": "echo", "payload": {}})
    jid = r.json()["id"]
    import asyncio
    await asyncio.sleep(0.1)
    r = await client.post(f"/jobs/{jid}/cancel")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "ALREADY_TERMINAL"
