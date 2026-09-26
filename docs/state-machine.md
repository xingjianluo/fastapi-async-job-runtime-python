# 作业状态机与一致性规则

## 1. 状态集合

| 状态 | 含义 | 是否终态 |
| --- | --- | --- |
| `QUEUED` | 已接受并持久化，正在有界队列中等待 | 否 |
| `RUNNING` | 已被调度器认领，某个尝试正在执行 | 否 |
| `AWAITING_RETRY` | 一次尝试遇到可重试错误，正在指数退避 | 否 |
| `SUCCEEDED` | 作业成功完成，`result` 已持久化 | 是 |
| `FAILED` | 不可重试错误 / 重试耗尽 / 重启时发现进程在执行中死亡 | 是 |
| `CANCELLED` | 在排队中、退避中或执行中被取消 | 是 |
| `TIMED_OUT` | 排队等待超时，或执行尝试超时且重试耗尽 | 是 |

> 提交即落库：作业以 `QUEUED` 状态原子写入 SQLite，不存在“内存里可见、重启后消失”的中间态。

## 2. 状态流转图

```
                 提交
                  │
                  ▼
              ┌────────┐  排队超时(deadline_queue)
              │ QUEUED │ ───────────────────────────► TIMED_OUT(QUEUE_TIMEOUT)
              └────────┘
                  │ 调度器认领（CAS，仅 QUEUED/AWAITING_RETRY 可认领）
                  ▼
              ┌────────┐  处理器正常返回
              │ RUNNING│ ───────────────────────────► SUCCEEDED(NONE)
              └────────┘
        │ 可重试错误/尝试超时      │ 不可重试错误/未知异常
        │ 且 attempt<max_attempts │
        ▼                         ├──────────────────► FAILED(NON_RETRYABLE)
 ┌───────────────┐                │
 │ AWAITING_RETRY│                │ 可重试但 attempt==max_attempts
 └───────────────┘                ├──────────────────► FAILED(RETRYABLE)
        │ 退避到期后重新认领        │ 尝试超时且重试耗尽
        └──────────► RUNNING       └──────────────────► TIMED_OUT(TIMEOUT)

 任意非终态 ──取消(CAS)──► CANCELLED(CANCELLED)
 RUNNING（进程崩溃/重启未完成）──► FAILED(INTERRUPTED)
```

## 3. 状态转移的三条硬规则

1. **只做比较并设置（CAS）转移。** 每次状态更新都带
   `WHERE id = ? AND state IN (允许的前置状态)`，数据库返回未命中时转移无效。
   因此状态永远不会“回退”：终态之后任何成功/失败结果都会被丢弃并记录日志
   （`success_after_terminal_ignored` / `failure_after_terminal_ignored`）。
2. **同一作业同一进程最多被派发一次。** 单个 dispatcher 协程从持久化队列拉取
   到期作业，并维护 `_in_flight` 去重；CAS 认领是第二道防线。取消、排队超时
   与派发之间的竞争只会有一个赢家。
3. **终态优先。** 取消与执行结果竞争时：
   - 取消先落库为 `CANCELLED`：执行结果写入的 CAS（要求 `RUNNING`）失败，被忽略，
     在途协程同时收到 `asyncio.Task.cancel()` 尽快停止；
   - 成功/失败先落库：取消的 CAS 失败，接口返回 `409 TERMINAL_STATE_CONFLICT`。

   两种顺序都给出确定结论，不依赖时序。

## 4. 幂等规则

幂等键可以放在请求体 `idempotency_key`，也可以放在 HTTP 头 `Idempotency-Key`。
键在数据库上有唯一约束。

提交时计算 `payload_hash = sha256(json({type, payload}, 排序键))`：

| 情况 | 结论 | HTTP |
| --- | --- | --- |
| 键不存在 | 创建新作业，`reused=false` | `201` |
| 键已存在且作业非终态、`payload_hash` 相同 | 返回同一作业，`reused=true` | `201` |
| 键已存在且作业已在任意终态（无论载荷是否相同） | 拒绝：终态冲突（优先级最高） | `409 TERMINAL_STATE_CONFLICT` |
| 键已存在、作业非终态但 `payload_hash` 不同（含 job_type 不同） | 拒绝：载荷冲突 | `409 IDEMPOTENCY_PAYLOAD_CONFLICT` |

并发同键提交依赖唯一约束保证只有一个 `INSERT` 成功，其余请求回读后按上表处理。

取消操作同样拒绝终态作业，返回 `409 TERMINAL_STATE_CONFLICT`，与“找不到作业”的
`404 JOB_NOT_FOUND` 区分。

## 5. 错误分类（ErrorClass）

| 分类 | 触发条件 | 是否重试 |
| --- | --- | --- |
| `NONE` | 成功或尚未失败 | — |
| `RETRYABLE` | 处理器抛出 `JobExecutionError(retryable=True)` | 是 |
| `NON_RETRYABLE` | `JobExecutionError(retryable=False)`；未分类的其他异常（异常类型写入 message） | 否，立即失败 |
| `TIMEOUT` | 单次执行超过 `attempt_timeout` | 是，耗尽后终态为 `TIMED_OUT` |
| `QUEUE_TIMEOUT` | `QUEUED` 等待超过 `queue_timeout`（作业从未执行） | 否，直接终态 `TIMED_OUT` |
| `CANCELLED` | 客户端取消 | 否，终态 `CANCELLED` |
| `INTERRUPTED` | 重启时发现作业停留在 `RUNNING` | 否，终态 `FAILED`，防止重复执行 |
| `PAYLOAD_CONFLICT` | 仅用于幂等冲突日志，不会成为作业状态 | — |

## 6. 重试与退避

- 每个作业有 `max_attempts`（默认 3，提交时可覆盖，范围 1–100）。
- 只有 `RETRYABLE` 与 `TIMEOUT` 会触发重试。
- 第 `n` 次尝试（n ≥ 2）前的等待：

  `delay(n) = min(backoff_base_delay * backoff_factor^(n-2), backoff_max_delay)`

- 退避期间作业处于 `AWAITING_RETRY`，此时取消会立即生效且后续尝试不再执行。
- 每次尝试都在 `attempt_history` 中追加一条记录（attempt、outcome、
  error_class、起止时间、message），状态与分类全程可追溯。

## 7. 有界并发、排队与超时

- `max_concurrency`：同时执行的作业上限。单 dispatcher 只在有空位时认领作业。
- `max_queue_size`：`QUEUED` 作业数上限。超过上限的提交会短暂等待空位
  （默认最多 2 秒），而不是被静默丢弃；持续饱和时返回 `503 QUEUE_FULL`。
- `queue_timeout`：作业在队列中的最长等待。到期由 dispatcher 确定性地置为
  `TIMED_OUT / QUEUE_TIMEOUT`，`attempt=0`（从未执行）。
- `attempt_timeout`：单次尝试的执行时限，由 `asyncio.wait_for` 强制执行。

## 8. 重启恢复（持久化）

启动时先做恢复、再启动 dispatcher：

- `RUNNING`（进程崩溃 / `kill -9` / 断电，未写关停标记）→
  `FAILED(INTERRUPTED)`：尝试不重放，保证不重复执行；
- `RUNNING`（有序关停：`shutdown()` 先持久化关停标记再中止在途尝试）→
  `CANCELLED(CANCELLED)`，语义为“服务关停主动中止”，与崩溃区分；关停标记在
  恢复时一次性消费，之后再崩溃仍按 `INTERRUPTED` 处理；
- `QUEUED`：把排队时钟（`enqueued_at/not_before/deadline_queue`）重置到新进程的
  单调时钟，重新排队，不会被旧时钟瞬间误判超时，也不会丢失；
- `AWAITING_RETRY`：保留已用尝试次数，按新配置重新计算下次退避到期时间后继续。

所有终态结果（含 `result` 与 `attempt_history`）保存在 SQLite 文件中，重启后
照常可查；幂等键表同样持久化，重启后的重复提交仍被正确拒绝或复用。
