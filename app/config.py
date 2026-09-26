"""运行时配置项（并发、排队、超时、重试），全部可通过环境变量覆盖。"""

from __future__ import annotations

import os
from dataclasses import dataclass

# 允许单作业覆盖的安全上限，避免请求把资源拉满。
MAX_RUN_TIMEOUT_CAP = 3600.0
MAX_QUEUE_TIMEOUT_CAP = 3600.0
MAX_ATTEMPTS_CAP = 20


def _env_str(name: str, default: str) -> str:
    raw = os.environ.get(name)
    return raw if raw not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    """作业运行时配置，所有时间单位均为秒。

    配置项：
    - db_path: SQLite 数据库文件路径（:memory: 仅用于测试单连接场景）
    - max_concurrency: 同时执行的作业上限（有界并发）
    - max_queue_size: 等待队列容量上限，超出后提交被明确拒绝（QUEUE_FULL）
    - queue_timeout: 排队最长等待时间，超时进入终态 TIMED_OUT
    - run_timeout: 单次执行超时时间；可重试时按尝试计
    - max_attempts: 最大尝试次数（1 表示不重试）
    - retry_base_delay / retry_max_delay: 指数退避基数与上限
    - poll_interval: worker 轮询领取间隔（同时用事件唤醒降低延迟）
    """

    db_path: str = "jobs.db"
    max_concurrency: int = 4
    max_queue_size: int = 1000
    queue_timeout: float = 300.0
    run_timeout: float = 60.0
    max_attempts: int = 3
    retry_base_delay: float = 0.2
    retry_max_delay: float = 5.0
    poll_interval: float = 0.05

    @staticmethod
    def from_env() -> Settings:
        return Settings(
            db_path=_env_str("JOB_DB_PATH", "jobs.db"),
            max_concurrency=max(1, _env_int("JOB_MAX_CONCURRENCY", 4)),
            max_queue_size=max(0, _env_int("JOB_MAX_QUEUE_SIZE", 1000)),
            queue_timeout=max(0.1, _env_float("JOB_QUEUE_TIMEOUT", 300.0)),
            run_timeout=max(0.05, _env_float("JOB_RUN_TIMEOUT", 60.0)),
            max_attempts=max(1, _env_int("JOB_MAX_ATTEMPTS", 3)),
            retry_base_delay=max(0.0, _env_float("JOB_RETRY_BASE_DELAY", 0.2)),
            retry_max_delay=max(0.0, _env_float("JOB_RETRY_MAX_DELAY", 5.0)),
            poll_interval=max(0.005, _env_float("JOB_POLL_INTERVAL", 0.05)),
        )


settings = Settings.from_env()
