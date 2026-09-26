"""Smoke test: the app imports and preserves the original root endpoint."""

import tempfile
from pathlib import Path

from fastapi.testclient import TestClient


def test_root_endpoint(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        monkeypatch.setenv(
            "JOB_RUNTIME_DB", str(Path(tmp) / "smoke.db")
        )
        import main
        importlib = __import__("importlib")
        importlib.reload(main)
        with TestClient(main.app) as client:
            resp = client.get("/")
        assert resp.status_code == 200
        assert resp.json() == {"Hello": "World"}


def test_openapi_exposes_jobs_routes(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        monkeypatch.setenv(
            "JOB_RUNTIME_DB", str(Path(tmp) / "smoke2.db")
        )
        import importlib
        import main
        importlib.reload(main)
        spec = main.app.openapi()
        paths = spec["paths"]
        assert "/" in paths
        assert "/jobs" in paths
        assert "/jobs/{job_id}" in paths
        assert "/jobs/{job_id}/cancel" in paths
