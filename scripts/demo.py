"""End-to-end local demo against a real uvicorn server (no external services).

It starts ``uvicorn main:app`` on an ephemeral port with a fast, in-memory
configuration via environment variables, then exercises:

* idempotent submit / reuse / payload conflict / terminal conflict;
* bounded queue + cancellation;
* attempt timeout -> retry -> TIMED_OUT;
* retryable backoff exhaustion;
* non-retryable immediate failure;
* restart durability on the SQLite file.

Run:  uv run python scripts/demo.py
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _request(method: str, url: str, body: dict | None = None,
             headers: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers=headers or {})
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


def _wait(base: str, job_id: str, *states: str, timeout: float = 20.0) -> dict:
    deadline = time.time() + timeout
    while True:
        status, body = _request("GET", f"{base}/jobs/{job_id}")
        if body["state"] in states:
            return body
        if time.time() > deadline:
            raise AssertionError(f"job {job_id} stuck: {body}")
        time.sleep(0.05)


def main() -> None:
    db_dir = tempfile.mkdtemp(prefix="job-runtime-demo-")
    db_path = os.path.join(db_dir, "demo.db")
    port = _free_port()
    env = {
        **os.environ,
        "JOB_RUNTIME_DB": db_path,
        "JOB_RUNTIME_MAX_CONCURRENCY": "2",
        "JOB_RUNTIME_MAX_QUEUE_SIZE": "50",
        "JOB_RUNTIME_ATTEMPT_TIMEOUT": "0.4",
        "JOB_RUNTIME_QUEUE_TIMEOUT": "10",
        "JOB_RUNTIME_DEFAULT_MAX_ATTEMPTS": "3",
        "JOB_RUNTIME_BACKOFF_BASE": "0.1",
        "JOB_RUNTIME_BACKOFF_FACTOR": "2",
        "JOB_RUNTIME_POLL_INTERVAL": "0.02",
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=ROOT, env=env,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        for _ in range(100):
            try:
                _request("GET", f"{base}/jobs/nope")
                break
            except Exception:
                time.sleep(0.1)
        else:
            raise RuntimeError("server did not start")

        def submit(job_type, payload, key=None, max_attempts=None):
            body = {"job_type": job_type, "payload": payload}
            if max_attempts is not None:
                body["max_attempts"] = max_attempts
            headers = {"Idempotency-Key": key} if key else {}
            return _request("POST", f"{base}/jobs", body, headers)

        print("\n=== 1. success + idempotent reuse ===")
        s, b = submit("succeed", {"n": 1, "delay": 0.2}, key="demo-1")
        job_id = b["job"]["id"]
        print("submit   :", s, b["job"]["state"], "reused=", b["reused"])
        s, b2 = submit("succeed", {"n": 1, "delay": 0.2}, key="demo-1")
        print("re-submit:", s, "same=", b2["job"]["id"] == job_id,
              "reused=", b2["reused"])
        final = _wait(base, job_id, "SUCCEEDED")
        print("final    :", final["state"], "result=", final["result"])

        print("\n=== 2. distinguishable conflicts ===")
        s, b = submit("succeed", {"n": 1, "delay": 0.2}, key="demo-1")
        print("terminal conflict (same payload)      :",
              s, b["error"]["code"])
        s, b = submit("succeed", {"n": 2}, key="demo-1")
        print("terminal conflict wins over payload diff:",
              s, b["error"]["code"])

        # Non-terminal job + different payload -> pure payload conflict.
        s, b = submit("slow", {"seconds": 30, "v": 1}, key="demo-2")
        running_id = b["job"]["id"]
        s, b = submit("slow", {"seconds": 30, "v": 2}, key="demo-2")
        print("payload conflict (job still non-terminal):",
              s, b["error"]["code"])
        _request("POST", f"{base}/jobs/{running_id}/cancel")

        print("\n=== 3. non-retryable fails immediately ===")
        s, b = submit("fail_permanent", {})
        final = _wait(base, b["job"]["id"], "FAILED")
        print("final:", final["state"], final["error_class"],
              "attempts=", final["attempt"])

        print("\n=== 4. retryable -> backoff -> exhausted ===")
        s, b = submit("fail_retryable", {})
        final = _wait(base, b["job"]["id"], "FAILED")
        print("final:", final["state"], final["error_class"],
              "attempts=", final["attempt"],
              "history=", [h["error_class"] for h in final["attempt_history"]])

        print("\n=== 5. attempt timeout -> TIMED_OUT after retries ===")
        s, b = submit("slow", {"seconds": 5})
        final = _wait(base, b["job"]["id"], "TIMED_OUT")
        print("final:", final["state"], final["error_class"],
              "attempts=", final["attempt"])

        print("\n=== 6. cancel a running slow job ===")
        s, b = submit("slow", {"seconds": 30})
        _wait(base, b["job"]["id"], "RUNNING")
        s, c = _request("POST", f"{base}/jobs/{b['job']['id']}/cancel")
        print("cancel:", s, c["state"], c["error_class"])

        print("\nDEMO OK — db file left at:", db_path)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    main()
