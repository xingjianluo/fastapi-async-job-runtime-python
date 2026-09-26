"""基础提交链路与幂等去重 / 冲突拒绝。"""

from __future__ import annotations

import pytest
from conftest import wait_for

from app.errors import ErrorCode, IdempotencyConflict
from app.models import JobState, JobSubmitRequest


@pytest.mark.asyncio
async def test_submit_and_succeed(manager):
    job, dedup = await manager.submit(
        JobSubmitRequest(job_type="echo", payload={"k": "v"})
    )
    assert dedup is False
    view = await wait_for(manager, job.id, {JobState.SUCCEEDED})
    assert view["state"] == "SUCCEEDED"
    assert view["terminal"] is True
    assert view["output"]["echoed"] == {"k": "v"}
    assert len(view["attempts"]) == 1


@pytest.mark.asyncio
async def test_idempotent_same_payload_returns_same_job(manager):
    req = JobSubmitRequest(
        job_type="echo", payload={"a": 1}, idempotency_key="KEY-1"
    )
    first, dedup1 = await manager.submit(req)
    assert dedup1 is False
    second, dedup2 = await manager.submit(req.model_copy(deep=True))
    assert dedup2 is True
    assert second.id == first.id

    view = await wait_for(manager, first.id, {JobState.SUCCEEDED})
    assert view["id"] == first.id


@pytest.mark.asyncio
async def test_idempotent_payload_conflict_rejected(manager):
    await manager.submit(
        JobSubmitRequest(job_type="echo", payload={"a": 1}, idempotency_key="KEY-2")
    )
    with pytest.raises(IdempotencyConflict) as exc:
        await manager.submit(
            JobSubmitRequest(job_type="echo", payload={"a": 2}, idempotency_key="KEY-2")
        )
    assert exc.value.error_code is ErrorCode.IDEMPOTENCY_CONFLICT
    assert exc.value.http_status == 409
    assert "existing_job_id" in exc.value.details


@pytest.mark.asyncio
async def test_idempotent_different_job_type_rejected(manager):
    await manager.submit(
        JobSubmitRequest(job_type="echo", payload={"x": 1}, idempotency_key="KEY-3")
    )
    with pytest.raises(IdempotencyConflict):
        await manager.submit(
            JobSubmitRequest(job_type="flaky", payload={"x": 1}, idempotency_key="KEY-3")
        )


@pytest.mark.asyncio
async def test_unknown_job_type_rejected(manager):
    from app.errors import UnknownJobType

    with pytest.raises(UnknownJobType) as exc:
        await manager.submit(JobSubmitRequest(job_type="nope", payload={}))
    assert exc.value.error_code is ErrorCode.UNKNOWN_JOB_TYPE


@pytest.mark.asyncio
async def test_get_missing_job_404(manager):
    from app.errors import JobNotFound

    with pytest.raises(JobNotFound) as exc:
        await manager.get("does-not-exist")
    assert exc.value.error_code is ErrorCode.JOB_NOT_FOUND


@pytest.mark.asyncio
async def test_http_idempotency_and_409(client):
    body = {"job_type": "echo", "payload": {"a": 1}, "idempotency_key": "HTTP-1"}
    r1 = await client.post("/jobs", json=body)
    assert r1.status_code == 201 and r1.json()["deduplicated"] is False
    r2 = await client.post("/jobs", json=body)
    assert r2.status_code == 201 and r2.json()["deduplicated"] is True
    assert r1.json()["id"] == r2.json()["id"]

    bad = dict(body, payload={"a": 2})
    r3 = await client.post("/jobs", json=bad)
    assert r3.status_code == 409
    assert r3.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"


@pytest.mark.asyncio
async def test_http_root_preserved(client):
    r = await client.get("/")
    assert r.status_code == 200
    assert r.json() == {"Hello": "World"}
