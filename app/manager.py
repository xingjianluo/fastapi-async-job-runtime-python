"""作业运行时核心：提交/查询/取消、有界并发、排队、超时与有限重试。

并发模型：固定数量的 asyncio worker + SQLite 条件更新领取任务。
SQLite 是唯一事实来源，内存事件仅用于唤醒 worker；进程崩溃后靠
启动恢复（RUNNING -> PENDING）保证不丢失。

调度规则（在既有能力之上叠加，不改变状态机与终态语义）：
- 优先级：high/normal/low 三档，同类型内高档先领取，同档先提交先执行；
- 类型隔离：每个作业类型可配置独立并发额度，额度只约束新领取，
  全局 max_concurrency（worker 数）仍是最外层硬约束；
- 等待提升（aging）：排队超过 aging_after_seconds 的作业按 HIGH 对待，
  保证等待过久的作业确定性地被提前，不会被无限期饿死。
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import logging
import uuid
from typing import Any

from app.config import (
    MAX_ATTEMPTS_CAP,
    MAX_QUEUE_TIMEOUT_CAP,
    MAX_RUN_TIMEOUT_CAP,
    Settings,
)
from app.errors import (
    AlreadyTerminal,
    ErrorCode,
    IdempotencyConflict,
    JobError,
    JobNotFound,
    QueueFull,
    UnknownJobType,
)
from app.handlers import JobHandler
from app.models import (
    DEFAULT_PRIORITY,
    PRIORITY_RANK,
    TERMINAL_STATES,
    AttemptRecord,
    FailureKind,
    JobRecord,
    JobResult,
    JobState,
    JobSubmitRequest,
    QueueReason,
)
from app.storage import DuplicateIdempotencyKey, JobStorage

logger = logging.getLogger("job_runtime")


def utcnow_iso() -> str:
    """统一的 UTC 时间戳（微秒精度、Z 后缀），保证库里字符串比较与时间一致。"""
    return _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _iso_add_seconds(base: _dt.datetime, seconds: float) -> str:
    return (base + _dt.timedelta(seconds=seconds)).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )


class JobManager:
    def __init__(
        self,
        storage: JobStorage,
        handlers: dict[str, JobHandler],
        settings: Settings,
    ) -> None:
        self.storage = storage
        self.handlers = handlers
        self.settings = settings
        self._workers: list[asyncio.Task[None]] = []
        self._watchdog: asyncio.Task[None] | None = None
        self._running: dict[str, asyncio.Task[None]] = {}
        self._wakeup = asyncio.Event()
        self._stop = asyncio.Event()
        self._started = False
        # 类型并发额度：可在运行时调整，仅影响之后的新领取，不打断在执行作业。
        self._type_quotas: dict[str, int] = dict(settings.type_quotas)

    # ------------------------------------------------------------------ 调度配置

    def get_scheduler_config(self) -> dict[str, Any]:
        """当前调度配置快照：全局并发、各类型额度、等待提升阈值。"""
        return {
            "max_concurrency": self.settings.max_concurrency,
            "type_quotas": dict(self._type_quotas),
            "aging_after_seconds": self.settings.aging_after_seconds,
        }

    def set_type_quotas(self, quotas: dict[str, int | None]) -> dict[str, Any]:
        """运行时调整类型额度。值为 None 表示移除该类型的额度限制。

        只影响之后新领取的作业；已在执行的不被打断。调整后立即唤醒 worker，
        让因额度放开的等待作业尽快被领取。
        """
        for job_type, quota in quotas.items():
            if quota is None:
                self._type_quotas.pop(job_type, None)
                logger.info("类型额度移除：type=%s 不再受类型额度限制", job_type)
                continue
            if not isinstance(quota, int) or isinstance(quota, bool) or quota < 0:
                raise JobError(
                    f"类型 {job_type} 的额度必须是非负整数，收到 {quota!r}",
                    error_code=ErrorCode.VALIDATION_ERROR,
                    http_status=422,
                )
            old = self._type_quotas.get(job_type)
            self._type_quotas[job_type] = quota
            logger.info(
                "类型额度调整：type=%s quota=%s->%d（仅影响新领取）",
                job_type, old if old is not None else "无限制", quota,
            )
        self._notify()
        return self.get_scheduler_config()

    def _excluded_types(self) -> frozenset[str]:
        """本轮领取中额度已满（或被暂停）的类型。"""
        if not self._type_quotas:
            return frozenset()
        running = self.storage.count_running_by_type()
        return frozenset(
            t for t, quota in self._type_quotas.items()
            if running.get(t, 0) >= quota
        )

    def _aging_cutoff(self, now: _dt.datetime) -> str:
        """等待提升截止时间：created_at 早于该值的等待作业按 HIGH 对待。"""
        return _iso_add_seconds(now, -self.settings.aging_after_seconds)

    # ------------------------------------------------------------------ 生命周期

    async def start(self) -> None:
        if self._started:
            return
        self.storage.init_schema()
        recovered = self.storage.requeue_running_jobs(utcnow_iso())
        if recovered:
            logger.warning(
                "重启恢复：%d 个处于 RUNNING 的作业被重新置为 PENDING", recovered
            )
        self._started = True
        self._stop.clear()
        self._wakeup = asyncio.Event()
        self._workers = [
            asyncio.create_task(self._worker_loop(i), name=f"job-worker-{i}")
            for i in range(self.settings.max_concurrency)
        ]
        self._watchdog = asyncio.create_task(
            self._watchdog_loop(), name="job-queue-watchdog"
        )
        self._notify()

    async def shutdown(self) -> None:
        if not self._started:
            return
        self._stop.set()
        self._wakeup.set()
        tasks = list(self._workers)
        if self._watchdog is not None:
            tasks.append(self._watchdog)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._watchdog = None
        self._workers = []
        self._started = False

    def _notify(self) -> None:
        self._wakeup.set()

    async def _watchdog_loop(self) -> None:
        """独立看门狗：周期收口排队超时作业并唤醒空闲 worker。

        即便所有 worker 都被长作业占满，排队超时仍能被确定性地处理，
        且超时进入终态后唤醒空闲 worker 继续领取其它可执行作业。
        """
        while not self._stop.is_set():
            try:
                now_iso = utcnow_iso()
                expired = self.storage.timeout_queued_jobs(now_iso)
                for eid in expired:
                    self._log_transition(
                        eid, "QUEUED", JobState.TIMED_OUT, 0,
                        "排队超过 queue_timeout，进入终态",
                        error_code=ErrorCode.QUEUE_TIMEOUT.value,
                    )
                if expired:
                    self._notify()
                await asyncio.sleep(self.settings.poll_interval)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("看门狗轮询异常")
                await asyncio.sleep(self.settings.poll_interval)

    # ------------------------------------------------------------------ 对外操作

    async def submit(self, request: JobSubmitRequest) -> tuple[JobRecord, bool]:
        """返回 (作业记录, 是否为幂等去重命中)。"""
        if request.job_type not in self.handlers:
            raise UnknownJobType(request.job_type, sorted(self.handlers))

        max_attempts = min(
            request.max_attempts or self.settings.max_attempts, MAX_ATTEMPTS_CAP
        )
        run_timeout = min(request.run_timeout or self.settings.run_timeout, MAX_RUN_TIMEOUT_CAP)
        queue_timeout = min(
            request.queue_timeout or self.settings.queue_timeout, MAX_QUEUE_TIMEOUT_CAP
        )

        if request.idempotency_key is not None:
            existing = self.storage.get_by_idempotency_key(request.idempotency_key)
            if existing is not None:
                return self._resolve_idempotent(existing, request)

        if self.storage.count_waiting() >= self.settings.max_queue_size:
            raise QueueFull(self.settings.max_queue_size)

        priority = request.priority or DEFAULT_PRIORITY
        now = _dt.datetime.now(_dt.UTC)
        now_iso = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        job_id = uuid.uuid4().hex
        job = JobRecord(
            id=job_id,
            job_type=request.job_type,
            payload=dict(request.payload),
            idempotency_key=request.idempotency_key,
            state=JobState.PENDING,
            attempt=1,
            max_attempts=max_attempts,
            created_at=now_iso,
            updated_at=now_iso,
            priority=PRIORITY_RANK[priority],
            not_before=now_iso,
            queued_deadline=_iso_add_seconds(now, queue_timeout),
        )
        job.payload["__run_timeout"] = run_timeout
        job.payload["__queue_timeout"] = queue_timeout
        try:
            self.storage.insert_job(job)
        except DuplicateIdempotencyKey:
            # 并发提交同一幂等键：重新查询并走幂等判定。
            existing = self.storage.get_by_idempotency_key(request.idempotency_key or "")
            assert existing is not None
            return self._resolve_idempotent(existing, request)

        self._log_transition(
            job_id, "-", JobState.PENDING, 1, "作业已提交并入队",
            priority=priority.value,
        )
        self._notify()
        # 隐藏字段不对外暴露
        public_payload = {k: v for k, v in job.payload.items() if not k.startswith("__")}
        job.payload = public_payload
        return job, False

    def _resolve_idempotent(
        self, existing: JobRecord, request: JobSubmitRequest
    ) -> tuple[JobRecord, bool]:
        incoming = dict(request.payload)
        stored = {k: v for k, v in existing.payload.items() if not k.startswith("__")}
        if incoming != stored or request.job_type != existing.job_type:
            raise IdempotencyConflict(
                "同一幂等键已被使用，但作业类型或载荷不一致",
                existing_job_id=existing.id,
            )
        self._log_transition(
            existing.id, existing.state.value, existing.state, existing.attempt,
            "幂等去重命中，返回既有作业",
        )
        existing.payload = stored
        return existing, True

    async def get(self, job_id: str) -> JobRecord:
        job = self.storage.get_job(job_id)
        if job is None:
            raise JobNotFound(job_id)
        job.queue_reason = self._queue_reason(job)
        return self._strip_internal(job)

    def _queue_reason(self, job: JobRecord) -> str | None:
        """等待中作业的排队原因（实时计算，不落库）；非等待状态返回 None。"""
        if job.state not in (JobState.PENDING, JobState.RETRY_WAIT):
            return None
        now = _dt.datetime.now(_dt.UTC)
        now_iso = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if job.not_before is not None and job.not_before > now_iso:
            return QueueReason.RETRY_BACKOFF.value
        quota = self._type_quotas.get(job.job_type)
        if quota is not None:
            running = self.storage.count_running_by_type()
            if running.get(job.job_type, 0) >= quota:
                return QueueReason.TYPE_QUOTA_FULL.value
        if self.storage.has_higher_priority_waiting(
            job.priority, job.created_at, now_iso, self._aging_cutoff(now)
        ):
            return QueueReason.WAITING_HIGHER_PRIORITY.value
        return QueueReason.QUEUED.value

    async def cancel(self, job_id: str) -> JobRecord:
        job = self.storage.get_job(job_id)
        if job is None:
            raise JobNotFound(job_id)
        if job.state == JobState.CANCELLED:
            raise AlreadyTerminal(job_id, JobState.CANCELLED.value, "cancel")
        if job.state in TERMINAL_STATES:
            raise AlreadyTerminal(job_id, job.state.value, "cancel")
        ok = self.storage.compare_and_set_state(
            job_id,
            {JobState.PENDING, JobState.RETRY_WAIT, JobState.RUNNING},
            JobState.CANCELLED,
            finished_at=utcnow_iso(),
            updated_at=utcnow_iso(),
            error_code="CANCELLED",
            error_message="用户取消",
        )
        if not ok:
            latest = self.storage.get_job(job_id)
            state = latest.state.value if latest else "UNKNOWN"
            raise AlreadyTerminal(job_id, state, "cancel")
        self._log_transition(job_id, job.state.value, JobState.CANCELLED, job.attempt,
                             "取消请求生效")
        self._notify()
        running_task = self._running.get(job_id)
        if running_task is not None:
            # 中断正在执行的协程；终态已先落库，取消不会导致重复执行或状态回退。
            running_task.cancel(msg="user_cancel")
        return self._strip_internal(self.storage.get_job(job_id))

    async def list_jobs(self, limit: int = 100) -> list[JobRecord]:
        return [self._strip_internal(j) for j in self.storage.list_jobs(limit)]

    @staticmethod
    def _strip_internal(job: JobRecord) -> JobRecord:
        job.payload = {k: v for k, v in job.payload.items() if not k.startswith("__")}
        return job

    # ------------------------------------------------------------------ worker

    async def _worker_loop(self, worker_id: int) -> None:
        while not self._stop.is_set():
            job: JobRecord | None = None
            try:
                now = _dt.datetime.now(_dt.UTC)
                now_iso = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                aging_cutoff = self._aging_cutoff(now)
                excluded = self._excluded_types()
                candidate = self.storage.peek_next_due(now_iso, excluded, aging_cutoff)
                if candidate is None:
                    try:
                        await asyncio.wait_for(
                            self._wakeup.wait(), timeout=self.settings.poll_interval
                        )
                        self._wakeup.clear()
                    except TimeoutError:
                        pass
                    continue
                run_timeout = float(
                    candidate.payload.get("__run_timeout", self.settings.run_timeout)
                )
                run_deadline_iso = _iso_add_seconds(now, run_timeout)
                # claim 与看门狗的 timeout 收口互斥（deadline>now / <=now）。
                job = self.storage.claim_next(
                    now_iso, run_deadline_iso, excluded, aging_cutoff
                )
                if job is None:
                    continue
                self._log_claim(worker_id, job, aging_cutoff)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("worker=%d 领取作业失败", worker_id)
                await asyncio.sleep(self.settings.poll_interval)
                continue

            task = asyncio.create_task(self._run_job(job))
            self._running[job.id] = task
            try:
                # shield：worker 自身被 shutdown 取消时，不连带再次取消子任务
                # （子任务的取消由 shutdown 统一处理，避免 cancelled twice）。
                await asyncio.shield(task)
            except asyncio.CancelledError:
                if not self._stop.is_set():
                    raise
                # shutdown 进行中：等待子任务走完它自己的取消/恢复分支。
                await asyncio.wait({task})
            except Exception:
                logger.exception("job=%s 执行循环异常", job.id)
            finally:
                self._running.pop(job.id, None)

    # ------------------------------------------------------------------ 执行

    def _log_claim(self, worker_id: int, job: JobRecord, aging_cutoff: str) -> None:
        """领取留痕：优先级、是否被等待提升、类型额度占用，便于排查调度行为。"""
        boosted = job.created_at <= aging_cutoff
        quota = self._type_quotas.get(job.job_type)
        running = self.storage.count_running_by_type().get(job.job_type, 0)
        logger.info(
            "job=%s worker=%d 领取作业 type=%s priority=%s(%d) boosted=%s "
            "type_running=%d/%s reason=%s",
            job.id, worker_id, job.job_type, job.priority_name, job.priority,
            boosted, running, quota if quota is not None else "无限制",
            "等待提升" if boosted else f"优先级{job.priority_name}",
        )

    async def _run_job(self, job: JobRecord) -> None:
        run_timeout = float(job.payload.get("__run_timeout", self.settings.run_timeout))
        loop = asyncio.get_running_loop()
        deadline_monotonic = loop.time() + run_timeout
        ctx_cls = self._make_context(job, deadline_monotonic)

        self._log_transition(job.id, job.state.value, JobState.RUNNING, job.attempt,
                             "开始执行", run_timeout=run_timeout)
        started_iso = utcnow_iso()

        try:
            output = await asyncio.wait_for(
                self._execute_attempt(job, ctx_cls), timeout=run_timeout
            )
        except asyncio.CancelledError:
            # 可能是用户取消（终态已是 CANCELLED）或 shutdown。
            latest = self.storage.get_job(job.id)
            if latest is not None and latest.state not in TERMINAL_STATES:
                # shutdown 中断：保持 RUNNING，由下次启动恢复为 PENDING。
                logger.info("job=%s 因服务停止中断，等待重启恢复", job.id)
            else:
                self._log_transition(
                    job.id, "RUNNING", JobState.CANCELLED, job.attempt,
                    "执行协程已随取消中断",
                )
            raise
        except TimeoutError:
            result = self._failure_result(
                FailureKind.TIMEOUT,
                ErrorCode.ATTEMPT_TIMEOUT.value,
                f"执行超过 run_timeout={run_timeout}s",
                "asyncio.TimeoutError",
            )
            self._handle_failure(job, result, started_iso)
            return
        except Exception as exc:  # noqa: BLE001 - 未分类异常按“不可重试”兜底，确保有确定结论
            # 处理器抛出的业务异常分类：RetryableJobError / 其它均不可重试。
            kind_attr = getattr(exc, "failure_kind", None)
            if kind_attr == FailureKind.RETRYABLE:
                kind = FailureKind.RETRYABLE
                code = ErrorCode.RETRYABLE_FAILURE.value
            else:
                kind = FailureKind.NON_RETRYABLE
                code = ErrorCode.NON_RETRYABLE_FAILURE.value
            result = self._failure_result(
                kind, code, str(exc) or exc.__class__.__name__,
                exc.__class__.__name__,
            )
            self._handle_failure(job, result, started_iso)
            return
        else:
            # 成功落库前再次 CAS：若取消/排队超时已先发生，则不覆盖终态。
            ok = self.storage.compare_and_set_state(
                job.id,
                {JobState.RUNNING},
                JobState.SUCCEEDED,
                output=output,
                error_code=None,
                error_message=None,
                failure_kind=None,
                finished_at=utcnow_iso(),
                updated_at=utcnow_iso(),
            )
            self._record_attempt(
                job.id, job.attempt, started_iso,
                JobResult(ok=True, output=output),
                state_after=JobState.SUCCEEDED.value if ok else None,
            )
            if ok:
                self._log_transition(job.id, "RUNNING", JobState.SUCCEEDED,
                                     job.attempt, "执行成功")
            else:
                logger.warning("job=%s 尝试成功但终态已被取消/超时占据，成功结果不覆盖",
                               job.id)
            self._notify()

    def _make_context(self, job: JobRecord, deadline_monotonic: float):
        from app.handlers import JobContext

        return JobContext(
            job_id=job.id,
            attempt=job.attempt,
            run_deadline_monotonic=deadline_monotonic,
        )

    async def _execute_attempt(self, job: JobRecord, ctx: Any) -> Any:
        handler = self.handlers[job.job_type]
        visible_payload = {k: v for k, v in job.payload.items() if not k.startswith("__")}
        return await handler(visible_payload, ctx)

    @staticmethod
    def _failure_result(kind: FailureKind, code: str, message: str,
                        error_type: str) -> JobResult:
        return JobResult(
            ok=False,
            failure_kind=kind,
            error_message=message,
            error_type=error_type,
        )

    def _record_attempt(self, job_id: str, attempt: int, started_iso: str,
                        result: JobResult, state_after: str | None = None) -> None:
        latest = self.storage.get_job(job_id)
        after = state_after or (latest.state.value if latest else "UNKNOWN")
        self.storage.append_attempt(
            job_id,
            AttemptRecord(
                attempt=attempt,
                started_at=started_iso,
                finished_at=utcnow_iso(),
                state_after=after,
                result=result,
            ),
        )
        if result is not None and not result.ok:
            self._log_transition(
                job_id, f"ATTEMPT#{attempt}", JobState.RUNNING, attempt,
                "尝试结束（失败，待分类处理）",
                failure_kind=result.failure_kind.value if result.failure_kind else "-",
                error_type=result.error_type or "-",
                error=result.error_message or "-",
            )

    def _terminal_code_for(self, kind: FailureKind | None, has_attempts_left: bool) -> tuple[JobState, str, str]:
        """根据失败分类与剩余尝试次数确定终态、错误码与说明。"""
        if kind == FailureKind.NON_RETRYABLE:
            return (
                JobState.FAILED,
                ErrorCode.NON_RETRYABLE_FAILURE.value,
                "不可重试错误，立即终态失败",
            )
        if kind == FailureKind.TIMEOUT and not has_attempts_left:
            return (
                JobState.TIMED_OUT,
                ErrorCode.ATTEMPT_TIMEOUT.value,
                "执行超时且重试次数耗尽",
            )
        return (
            JobState.FAILED,
            ErrorCode.RETRYABLE_FAILURE.value,
            "可重试错误次数已耗尽，终态失败",
        )

    def _handle_failure(self, job: JobRecord, result: JobResult,
                        started_iso: str) -> None:
        """按失败分类与剩余尝试次数，决定退避重试还是终态失败/超时。"""
        kind = result.failure_kind
        can_retry = kind in (FailureKind.RETRYABLE, FailureKind.TIMEOUT)
        has_attempts_left = job.attempt < job.max_attempts

        if can_retry and has_attempts_left:
            delay = min(
                self.settings.retry_base_delay * (2 ** (job.attempt - 1)),
                self.settings.retry_max_delay,
            )
            next_attempt = job.attempt + 1
            not_before = _iso_add_seconds(
                _dt.datetime.now(_dt.UTC), delay
            )
            ok = self.storage.compare_and_set_state(
                job.id,
                {JobState.RUNNING},
                JobState.RETRY_WAIT,
                attempt=next_attempt,
                not_before=not_before,
                run_deadline=None,
                error_code=ErrorCode.RETRYABLE_FAILURE.value
                if kind == FailureKind.RETRYABLE
                else ErrorCode.ATTEMPT_TIMEOUT.value,
                error_message=result.error_message,
                failure_kind=kind.value if kind else None,
                updated_at=utcnow_iso(),
            )
            self._record_attempt(
                job.id, job.attempt, started_iso, result,
                state_after=JobState.RETRY_WAIT.value,
            )
            if ok:
                self._log_transition(
                    job.id, "RUNNING", JobState.RETRY_WAIT, next_attempt,
                    f"{kind.value if kind else '-'}：{delay:.3f}s 后重试",
                )
            self._notify()
            return

        # 不可重试，或可重试次数已耗尽：进入终态。
        target, code, note = self._terminal_code_for(kind, has_attempts_left)
        ok = self.storage.compare_and_set_state(
            job.id,
            {JobState.RUNNING},
            target,
            run_deadline=None,
            error_code=code,
            error_message=result.error_message,
            failure_kind=kind.value if kind else None,
            finished_at=utcnow_iso(),
            updated_at=utcnow_iso(),
        )
        self._record_attempt(job.id, job.attempt, started_iso, result,
                             state_after=target.value)
        if ok:
            self._log_transition(job.id, "RUNNING", target, job.attempt, note,
                                 error_code=code)
        self._notify()

    def _log_transition(self, job_id: str, frm: str, to: JobState,
                        attempt: int, note: str = "", **extra: Any) -> None:
        logger.info(
            "job=%s attempt=%d state=%s->%s %s %s",
            job_id, attempt, frm, to.value, note,
            " ".join(f"{k}={v}" for k, v in extra.items()),
        )
