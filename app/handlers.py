"""内置作业类型与处理器注册表。

处理器签名：async def handler(payload, ctx) -> Any
- 正常返回：作业成功
- 抛出 RetryableJobError：可重试错误（退避后重试）
- 抛出 NonRetryableJobError 或其它异常：不可重试错误（立即终态失败）
- 被 asyncio.CancelledError 取消：取消生效或按执行超时归类
ctx 提供可被取消/超时打断的 sleep 与作业标识。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from app.errors import ErrorCode, JobError
from app.models import FailureKind


class RetryableJobError(JobError):
    """处理器主动声明的可重试失败。"""

    failure_kind = FailureKind.RETRYABLE

    def __init__(self, message: str, *, error_type: str | None = None) -> None:
        super().__init__(
            message,
            error_code=ErrorCode.RETRYABLE_FAILURE,
            http_status=200,
        )
        self.error_type = error_type or self.__class__.__name__


class NonRetryableJobError(JobError):
    """处理器主动声明的不可重试失败。"""

    failure_kind = FailureKind.NON_RETRYABLE

    def __init__(self, message: str, *, error_type: str | None = None) -> None:
        super().__init__(
            message,
            error_code=ErrorCode.NON_RETRYABLE_FAILURE,
            http_status=200,
        )
        self.error_type = error_type or self.__class__.__name__


@dataclass
class JobContext:
    """传给处理器的执行上下文。"""

    job_id: str
    attempt: int
    run_deadline_monotonic: float

    def remaining(self) -> float:
        return max(0.0, self.run_deadline_monotonic - asyncio.get_event_loop().time())

    async def sleep(self, seconds: float) -> None:
        """可被取消/超时打断的睡眠。超出执行预算时立即抛出超时错误。"""
        remaining = self.remaining()
        if seconds > remaining:
            raise TimeoutError(
                f"sleep {seconds}s 超过剩余执行预算 {remaining:.3f}s"
            )
        await asyncio.sleep(seconds)


JobHandler = Callable[[dict[str, Any], JobContext], Awaitable[Any]]


async def echo_handler(payload: dict[str, Any], ctx: JobContext) -> Any:
    """原样回显 payload，便于基本链路与幂等验证。"""
    return {"echoed": payload, "job_id": ctx.job_id, "attempt": ctx.attempt}


async def flaky_handler(payload: dict[str, Any], ctx: JobContext) -> Any:
    """前 fail_times 次尝试抛出可重试错误，之后成功，用于验证退避重试。

    payload:
    - fail_times: 前 N 次尝试可重试失败（默认 1）
    - message: 自定义错误信息
    - non_retryable: true 时立即抛出不可重试错误（用于验证快速失败）
    """
    if payload.get("non_retryable"):
        raise NonRetryableJobError(
            payload.get("message", "显式不可重试失败"),
            error_type="ConfiguredNonRetryable",
        )
    fail_times = int(payload.get("fail_times", 1))
    if ctx.attempt <= fail_times:
        raise RetryableJobError(
            payload.get("message", f"模拟可重试失败（第 {ctx.attempt} 次）"),
            error_type="FlakyTransient",
        )
    return {"ok_after_attempt": ctx.attempt, "job_id": ctx.job_id}


async def sleep_handler(payload: dict[str, Any], ctx: JobContext) -> Any:
    """睡眠 seconds 秒，配合 run_timeout 验证执行超时与取消。"""
    seconds = float(payload.get("seconds", 0.1))
    # 分段睡眠，确保取消信号能被及时观察到。
    step = 0.02
    waited = 0.0
    while waited < seconds:
        await ctx.sleep(min(step, seconds - waited))
        waited += step
    return {"slept": round(waited, 3), "job_id": ctx.job_id}


async def fail_handler(payload: dict[str, Any], ctx: JobContext) -> Any:
    """始终抛出不可重试的普通异常，验证未分类异常立即终态失败。"""
    kind = payload.get("kind", "plain")
    if kind == "retryable":
        raise RetryableJobError("fail_handler 配置为可重试", error_type="ConfiguredRetry")
    raise RuntimeError(payload.get("message", "fail_handler 确定性不可重试失败"))


def default_handlers() -> dict[str, JobHandler]:
    return {
        "echo": echo_handler,
        "flaky": flaky_handler,
        "sleep": sleep_handler,
        "fail": fail_handler,
    }
