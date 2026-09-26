"""End-to-end HTTP tests via FastAPI TestClient.

Verifies wire-level behavior: idempotency header, reused flag, distinct
error codes for the two conflict types, query after completion and cancel.
The original ``GET /`` behavior is covered in test_smoke.py.
"""

import time
from contextlib import asynccontextmanager

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from job_runtime.api import install_exception_handlers, router as jobs_router
from job_runtime.config import RuntimeConfig
from job_runtime.service import JobService


@pytest.fixture
def client(tmp_path):
    cfg = RuntimeConfig(
        db_path=str(tmp_path / "http.db"),
        max_concurrency=2,
        attempt_timeout=5.0,
        queue_timeout=60.0,
        default_max_attempts=2,
        backoff_base_delay=0.01,
        backoff_factor=2.0,
        poll_interval=0.005,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        svc = JobService(cfg)
        await svc.startup()
        app.state.job_service = svc
        try:
            yield
        finally:
            await svc.shutdown()
            svc.store.close()

    app = FastAPI(lifespan=lifespan)
    app.include_router(jobs_router)
    install_exception_handlers(app)

    with TestClient(app) as c:
        yield c


def _wait_state(client: TestClient, job_id: str, *states: str,
                timeout: float = 10.0) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        resp = client.get(f"/jobs/{job_id}")
        assert resp.status_code == 200
        body = resp.json()
        if body["state"] in states:
            return body
        if time.monotonic() > deadline:
            raise AssertionError(f"job never reached {states}: {body}")
        time.sleep(0.02)


def test_submit_and_query_success(client):
    resp = client.post("/jobs", json={
        "job_type": "succeed", "payload": {"hello": "world"},
    })
    assert resp.status_code == 201
    body = resp.json()
    assert body["reused"] is False
    job_id = body["job"]["id"]
    assert body["job"]["state"] in ("QUEUED", "RUNNING", "SUCCEEDED")

    final = _wait_state(client, job_id, "SUCCEEDED")
    assert final["result"]["echo"] == {"hello": "world"}
    assert final["error_class"] == "NONE"
    assert final["attempt_history"][-1]["outcome"] == "SUCCESS"


def test_idempotency_header_reuses_job(client):
    # Use a slow job so both submissions arrive while it is still
    # non-terminal; terminal re-submit is covered separately.
    headers = {"Idempotency-Key": "http-key-1"}
    body = {"job_type": "slow", "payload": {"seconds": 30}}
    r1 = client.post("/jobs", json=body, headers=headers)
    r2 = client.post("/jobs", json=body, headers=headers)
    assert r1.status_code == 201 and r2.status_code == 201
    assert r1.json()["job"]["id"] == r2.json()["job"]["id"]
    assert r1.json()["reused"] is False
    assert r2.json()["reused"] is True
    # cleanup so the lifespan shutdown is quick
    job_id = r1.json()["job"]["id"]
    _wait_state(client, job_id, "RUNNING")
    client.post(f"/jobs/{job_id}/cancel")


def test_payload_conflict_has_distinct_code(client):
    # slow job stays non-terminal so payload conflict is the applicable rule
    # (terminal re-submit is covered by test_terminal_resubmit_has_distinct_code)
    body_a = {"job_type": "slow", "payload": {"seconds": 30, "v": 1}}
    client.post("/jobs", json=body_a, headers={"Idempotency-Key": "conf"})
    resp = client.post(
        "/jobs",
        json={"job_type": "slow", "payload": {"seconds": 30, "v": 2}},
        headers={"Idempotency-Key": "conf"},
    )
    assert resp.status_code == 409
    err = resp.json()["error"]
    assert err["code"] == "IDEMPOTENCY_PAYLOAD_CONFLICT"
    assert err["message"]


def test_terminal_resubmit_has_distinct_code(client):
    first = client.post(
        "/jobs", json={"job_type": "fail_permanent"},
        headers={"Idempotency-Key": "term"},
    )
    job_id = first.json()["job"]["id"]
    _wait_state(client, job_id, "FAILED")

    resp = client.post("/jobs", json={"job_type": "fail_permanent"},
                       headers={"Idempotency-Key": "term"})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "TERMINAL_STATE_CONFLICT"


def test_get_missing_and_unknown_type(client):
    resp = client.get("/jobs/nope")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "JOB_NOT_FOUND"

    resp = client.post("/jobs", json={"job_type": "ghost"})
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "UNKNOWN_JOB_TYPE"


def test_cancel_flow(client):
    resp = client.post("/jobs", json={
        "job_type": "slow", "payload": {"seconds": 30},
    })
    job_id = resp.json()["job"]["id"]
    _wait_state(client, job_id, "RUNNING")
    cancelled = client.post(f"/jobs/{job_id}/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json()["state"] == "CANCELLED"
    assert cancelled.json()["error_class"] == "CANCELLED"

    again = client.post(f"/jobs/{job_id}/cancel")
    assert again.status_code == 409
    assert again.json()["error"]["code"] == "TERMINAL_STATE_CONFLICT"


def test_queue_saturation_returns_503(tmp_path):
    cfg = RuntimeConfig(
        db_path=str(tmp_path / "sat.db"),
        max_concurrency=1,
        max_queue_size=1,
        attempt_timeout=30.0,
        poll_interval=0.02,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        svc = JobService(cfg)
        await svc.startup()
        app.state.job_service = svc
        try:
            yield
        finally:
            await svc.shutdown()
            svc.store.close()

    app = FastAPI(lifespan=lifespan)
    app.include_router(jobs_router)
    install_exception_handlers(app)

    with TestClient(app) as c:
        # 1 running + 1 queued -> the next submit blocks for wait_for_slot
        # (default 2s) and then gets a distinguishable 503 QUEUE_FULL.
        c.post("/jobs", json={"job_type": "slow",
                              "payload": {"seconds": 30}})
        c.post("/jobs", json={"job_type": "slow",
                              "payload": {"seconds": 30}})
        # Poll until the queue really holds one waiter.
        deadline = time.monotonic() + 5
        while True:
            r = c.post("/jobs", json={"job_type": "succeed"})
            if r.status_code == 503:
                break
            # A slot freed unexpectedly; keep saturating.
            assert r.status_code == 201
            assert time.monotonic() < deadline
        assert r.json()["error"]["code"] == "QUEUE_FULL"
