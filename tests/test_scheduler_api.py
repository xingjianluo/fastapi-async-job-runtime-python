"""调度能力的 HTTP 接口：优先级提交、额度查询与运行时调整。"""

from __future__ import annotations

import pytest
from conftest import wait_for

from app.models import JobState


@pytest.mark.asyncio
async def test_submit_with_priority_via_api(client):
    resp = await client.post(
        "/jobs",
        json={"job_type": "echo", "payload": {"x": 1}, "priority": "high"},
    )
    assert resp.status_code == 201
    view = (await client.get(f"/jobs/{resp.json()['id']}")).json()
    assert view["priority"] == "high"


@pytest.mark.asyncio
async def test_submit_without_priority_keeps_default(client):
    # 老请求不带优先级：接口不报错，按 normal 处理。
    resp = await client.post("/jobs", json={"job_type": "echo", "payload": {}})
    assert resp.status_code == 201
    view = await wait_for(
        client, resp.json()["id"], {JobState.SUCCEEDED}, via_http=True
    )
    assert view["priority"] == "normal"
    assert view["queue_reason"] is None


@pytest.mark.asyncio
async def test_invalid_priority_rejected(client):
    resp = await client.post(
        "/jobs", json={"job_type": "echo", "priority": "urgent"}
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.asyncio
async def test_quota_update_and_readback(client):
    resp = await client.put("/scheduler/quotas", json={"quotas": {"echo": 1}})
    assert resp.status_code == 200
    assert resp.json()["type_quotas"] == {"echo": 1}

    cfg = (await client.get("/scheduler/config")).json()
    assert cfg["type_quotas"] == {"echo": 1}
    assert cfg["max_concurrency"] == 2  # 全局上限不受额度调整影响
    assert "aging_after_seconds" in cfg

    # 传 null 移除该类型的额度限制。
    resp = await client.put("/scheduler/quotas", json={"quotas": {"echo": None}})
    assert resp.status_code == 200
    assert resp.json()["type_quotas"] == {}


@pytest.mark.asyncio
async def test_invalid_quota_rejected(client):
    resp = await client.put("/scheduler/quotas", json={"quotas": {"echo": -1}})
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.asyncio
async def test_quota_zero_pauses_type_via_api(client):
    # 通过接口把 echo 额度调为 0：新作业排队且原因可见；恢复后执行。
    resp = await client.put("/scheduler/quotas", json={"quotas": {"echo": 0}})
    assert resp.status_code == 200

    resp = await client.post("/jobs", json={"job_type": "echo", "payload": {}})
    job_id = resp.json()["id"]
    view = (await client.get(f"/jobs/{job_id}")).json()
    assert view["state"] == "PENDING"
    assert view["queue_reason"] == "type_quota_full"

    await client.put("/scheduler/quotas", json={"quotas": {"echo": None}})
    view = await wait_for(client, job_id, {JobState.SUCCEEDED}, via_http=True)
    assert view["state"] == "SUCCEEDED"
