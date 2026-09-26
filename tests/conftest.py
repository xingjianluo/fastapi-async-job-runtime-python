"""Shared pytest fixtures.

A ``make_service`` async-generator factory is provided so each test can pick
concurrency, timeouts and backoff independently. Services use a temp-file
SQLite DB by default (so restart recovery can be tested with a second
service on the same file); pass ``in_memory=True`` for ephemeral tests.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from job_runtime.config import RuntimeConfig
from job_runtime.handlers import reset_flaky_counters
from job_runtime.log import configure_logging
from job_runtime.service import JobService


@pytest.fixture(autouse=True)
def _logging_and_counters():
    configure_logging(logging.INFO)
    reset_flaky_counters()
    yield
    reset_flaky_counters()


@pytest.fixture
def tmp_db(tmp_path: Path) -> str:
    return str(tmp_path / "jobs.db")


@pytest.fixture
async def make_service(tmp_db: str):
    services: list[JobService] = []

    async def _make(
        *,
        in_memory: bool = False,
        db_path: str | None = None,
        max_concurrency: int = 2,
        max_queue_size: int = 100,
        attempt_timeout: float = 5.0,
        queue_timeout: float = 600.0,
        default_max_attempts: int = 3,
        backoff_base_delay: float = 0.02,
        backoff_factor: float = 2.0,
        backoff_max_delay: float = 1.0,
        poll_interval: float = 0.005,
        auto_start_runner: bool = True,
    ) -> JobService:
        cfg = RuntimeConfig(
            max_concurrency=max_concurrency,
            max_queue_size=max_queue_size,
            attempt_timeout=attempt_timeout,
            queue_timeout=queue_timeout,
            default_max_attempts=default_max_attempts,
            backoff_base_delay=backoff_base_delay,
            backoff_factor=backoff_factor,
            backoff_max_delay=backoff_max_delay,
            poll_interval=poll_interval,
            db_path=db_path if db_path is not None
            else (":memory:" if in_memory else tmp_db),
        )
        svc = JobService(cfg, auto_start_runner=auto_start_runner,
                         log_level=logging.INFO)
        await svc.startup()
        services.append(svc)
        return svc

    yield _make

    for svc in services:
        if svc._started and not svc.store.closed:
            await svc.shutdown()
    for svc in services:
        try:
            svc.store.close()
        except Exception:
            pass
