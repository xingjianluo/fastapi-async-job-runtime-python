# 本地异步作业运行时（FastAPI + asyncio + SQLite）

一个**无需外部服务、无真实凭据**即可本地运行的异步作业运行时。作业在
并发提交、重复提交（幂等去重）、取消、执行超时、排队超时与失败重试等情况下，
都能得到**可解释、一致且可区分**的状态结论；所有状态落 SQLite，服务重启后
终态结果仍可查询、未完成作业不丢失。调度层支持**优先级（三档）**、
**按作业类型的并发额度隔离**与**等待提升（防饿死）**，类型额度可在
服务不重启的情况下在线调整。

- 运行时：Python 标准库 `asyncio`（固定 worker 池 + 独立看门狗）
- 持久化：SQLite 单文件（WAL），状态流转全部为条件更新（CAS）
- Web：FastAPI，保留了原有 `GET / -> {"Hello": "World"}` 的既有行为

---

## 1. 快速开始

```bash
# 安装依赖（需要 Python >= 3.14）
uv sync --group dev

# 启动服务（默认 127.0.0.1:8000，首次启动自动建库 jobs.db）
uv run uvicorn main:app --reload
# 或：uv run python main.py

# 跑测试
uv run pytest
```

### 提交与查询示例

```bash
# 提交（带幂等键）
curl -s -X POST http://127.0.0.1:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"job_type":"echo","payload":{"k":"v"},"idempotency_key":"key-1"}'

# 查询
curl -s http://127.0.0.1:8000/jobs/<job_id>

# 取消
curl -s -X POST http://127.0.0.1:8000/jobs/<job_id>/cancel

# 列表
curl -s "http://127.0.0.1:8000/jobs?limit=50"
```

提交响应：`{"id": "...", "state": "PENDING", "deduplicated": false}`
重复提交相同幂等键 + 相同载荷时返回同一作业，且 `deduplicated=true`。

### 内置作业类型（`app/handlers.py`）

| job_type | payload | 用途 |
|---|---|---|
| `echo` | 任意 | 原样回显，验证基本链路/幂等 |
| `flaky` | `fail_times`、`non_retryable`、`message` | 前 N 次可重试失败后成功；或显式不可重试 |
| `sleep` | `seconds` | 睡眠，用于执行超时/取消验证 |
| `fail` | `kind=retryable` 或缺省 | 可重试失败 / 确定性不可重试失败 |

也可在 `default_handlers()` 中注册自定义 `async def handler(payload, ctx)`。
处理器内用 `await ctx.sleep(...)` 睡眠（可被取消/超时打断）。

---

## 2. 作业状态机

```
                         提交
                          │
                          ▼
                     ┌─────────┐   到期被领取    ┌─────────┐  成功(处理器返回)
                     │ PENDING │ ─────────────► │ RUNNING │ ───────────────► SUCCEEDED
                     └─────────┘                 └─────────┘
                          ▲                          │  │
              重启恢复     │                          │  │ 可重试失败/执行超时
          (RUNNING→PENDING)│                         │  │ 且仍有剩余次数
                          │            退避到期重新入队│  ▼
                          │                          │ ┌────────────┐
                          └──────────────────────────┘ │ RETRY_WAIT │
                                                       └────────────┘
   PENDING / RETRY_WAIT / RUNNING ──用户取消──► CANCELLED
   RUNNING ──不可重试错误，或重试耗尽──► FAILED
   RUNNING ──执行超时且重试耗尽──► TIMED_OUT
   PENDING / RETRY_WAIT ──排队超过 queue_timeout──► TIMED_OUT
```

**终态（不可逆，不回退）**：`SUCCEEDED`、`FAILED`、`CANCELLED`、`TIMED_OUT`。
非终态：`PENDING`（排队中）、`RUNNING`（执行中）、`RETRY_WAIT`（退避等待重试）。

关键确定性规则：

- **原子领取**：worker 通过单条条件 `UPDATE ... RETURNING` 把一个到期作业
  从 `PENDING/RETRY_WAIT` 置为 `RUNNING`，只有更新成功的 worker 才执行它，
  因而**同一个作业不会被执行多次**。
- **终态写入用 CAS**：成功/失败/取消都要求“当前状态符合预期”才更新；
  取消与成功竞争时先落库者胜出，后者不会覆盖终态，**状态不回退**。
- **排队超时与领取互斥**：看门狗以同一时刻判定
  `queued_deadline <= now`（置 `TIMED_OUT`），worker 领取
  `queued_deadline > now` 的作业，二者互斥且穷尽，作业要么被执行、
  要么按排队超时进终态，**不会被静默丢弃**。看门狗独立运行，即使所有
  worker 都被长作业占满，排队超时依旧按时生效。
- **执行超时**：每次尝试用 `asyncio.wait_for(run_timeout)` 包裹；超时归类为
  `TIMEOUT`，若仍有剩余次数则退避重试，耗尽后终态 `TIMED_OUT`。
- **重启恢复**：启动时把遗留 `RUNNING` 原子置回 `PENDING`（崩溃时无法得知
  那次尝试是否生效，按 at-least-once 有界补跑）；排队中、从未开始的作业
  重启后只执行一次；终态与输出持久化，重启后照常可查。

---

## 3. 幂等与去重规则

- 请求可选 `idempotency_key`（不带键的提交总是新建作业）。
- 相同 `idempotency_key`：
  - **载荷与作业类型一致** → 返回同一作业，`deduplicated=true`（HTTP 201，
    不新建、不重复执行）。
  - **载荷或作业类型冲突** → 返回 `409 IDEMPOTENCY_CONFLICT`，响应中带
    `existing_job_id`，明确拒绝。
- 并发提交同一键：数据库对幂等键建**唯一部分索引**，并发下也只会有一个作业胜出，
  其余要么去重命中、要么收到冲突错误。
- 对**已进入终态**的作业重复操作：
  - 再次以同键同载荷提交：返回既有终态作业（去重命中）。
  - 取消已终态作业：返回 `409 ALREADY_TERMINAL`（已取消的再次取消同样如此）。

### 错误码（可区分的错误原因）

| code | HTTP | 含义 |
|---|---|---|
| `IDEMPOTENCY_CONFLICT` | 409 | 幂等键已存在但载荷/类型不一致 |
| `JOB_NOT_FOUND` | 404 | 作业不存在 |
| `ALREADY_TERMINAL` | 409 | 对终态作业执行取消等操作 |
| `QUEUE_FULL` | 429 | 等待队列达到 `JOB_MAX_QUEUE_SIZE` 上限 |
| `UNKNOWN_JOB_TYPE` | 400 | 提交了未注册的作业类型 |
| `VALIDATION_ERROR` | 422 | 请求体校验失败 |
| `NON_RETRYABLE_FAILURE` | —（终态字段） | 不可重试错误，立即 `FAILED` |
| `RETRYABLE_FAILURE` | —（终态字段） | 可重试错误，重试耗尽后 `FAILED` |
| `ATTEMPT_TIMEOUT` | —（终态字段） | 执行超时，重试耗尽后 `TIMED_OUT` |
| `QUEUE_TIMEOUT` | —（终态字段） | 排队超时，`TIMED_OUT` |

错误响应体形如：

```json
{"error": {"code": "IDEMPOTENCY_CONFLICT",
           "message": "同一幂等键已被使用，但作业类型或载荷不一致",
           "details": {"existing_job_id": "..."}}}
```

---

## 4. 失败分类与有限重试

处理器的三种结局：

1. 正常 `return`：作业 `SUCCEEDED`，输出持久化。
2. 抛 `RetryableJobError`：分类 `RETRYABLE`，按指数退避重试。
3. 抛 `NonRetryableJobError` 或其它任意异常：分类 `NON_RETRYABLE`，
   **立即终态 `FAILED`**，不重试。

执行超时单独归类为 `TIMEOUT`，同样参与有限重试。

- 最大尝试次数 `max_attempts`（含首次，`1` 表示不重试）。
- 退避（确定、无随机抖动，便于复现）：
  `delay = min(retry_base_delay * 2**(attempt-1), retry_max_delay)`。
- 可重试/超时错误在次数耗尽后进入终态（`FAILED` / `TIMED_OUT`）。
- 每次尝试都写入 `attempts` 留痕（开始/结束时间、`failure_kind`、
  `error_type`、错误信息），作业记录的终态字段与每次尝试分类保持一致。

---

## 5. 并发、排队与超时配置项

均可通过环境变量覆盖（前缀 `JOB_`），也可在 `Settings(...)` 构造时注入
（测试就是这样做的）。提交时还可对单个作业覆盖 `max_attempts`、
`run_timeout`、`queue_timeout`（受安全上限约束）。

| 环境变量 | 默认值 | 含义 |
|---|---|---|
| `JOB_DB_PATH` | `jobs.db` | SQLite 文件路径 |
| `JOB_MAX_CONCURRENCY` | `4` | 同时执行的作业上限（有界并发） |
| `JOB_MAX_QUEUE_SIZE` | `1000` | 等待队列容量；满了提交返回 `429 QUEUE_FULL` |
| `JOB_QUEUE_TIMEOUT` | `300` 秒 | 排队最长等待，超时终态 `TIMED_OUT` |
| `JOB_RUN_TIMEOUT` | `60` 秒 | 单次执行超时，可重试时按“每次尝试”计 |
| `JOB_MAX_ATTEMPTS` | `3` | 最大尝试次数（含首次） |
| `JOB_RETRY_BASE_DELAY` | `0.2` 秒 | 指数退避基数 |
| `JOB_RETRY_MAX_DELAY` | `5` 秒 | 退避上限 |
| `JOB_POLL_INTERVAL` | `0.05` 秒 | worker/看门狗轮询间隔（同时用事件唤醒降低延迟） |
| `JOB_TYPE_QUOTAS` | `{}` | 类型并发额度初始值，JSON 对象，如 `'{"batch":2,"report":1}'` |
| `JOB_AGING_THRESHOLD` | `60` 秒 | 等待提升阈值：每完整等待该时长有效优先级提升 1 档；`<=0` 关闭 |
| `JOB_AGING_MAX_BOOST` | `2` | 等待提升的最大档数（有效优先级不超过 HIGH=3） |

单作业覆盖的安全上限：`run_timeout <= 3600s`、`queue_timeout <= 3600s`、
`max_attempts <= 20`。

---

## 6. 分级调度与类型隔离

### 6.1 优先级

- 提交时可带 `priority`：`1=LOW`、`2=NORMAL`（默认）、`3=HIGH` 三档。
  **老请求不带 `priority` 时按 NORMAL 处理，接口行为与之前完全一致**。
- 领取次序：到期作业按**有效优先级降序**，同档按 `created_at` 升序
  （先提交先执行）。跨类型不保证严格全局排序（各类型额度独立判定），
  但同类型内高优先级一定先被领取。
- 优先级**不参与幂等判定**：同一幂等键 + 相同载荷/类型依旧去重命中，
  保留首次提交时的优先级。

### 6.2 类型并发额度（隔离）

- 每种作业类型可有独立并发额度（`PUT /scheduler/quotas` 在线调整，
  初始值来自 `JOB_TYPE_QUOTAS`）；未配置的类型只受全局并发约束。
- 某种类型额度占满时，该类型的新作业等待，**不影响其它类型被领取**
  （批处理跑满自己的额度，也堵不住别的小作业）。
- **全局 `JOB_MAX_CONCURRENCY` 仍是最外层硬约束**：各类型额度之和超过
  全局上限时，实际并发以全局上限为准。
- 额度调整**只影响之后新领取的作业**，已在执行的作业不被打断；
  调大后立即唤醒 worker 领取等待中的作业；额度调为 `0` 可暂停该类型。
- 取消/完成/失败释放的名额会及时让给等待中的同类作业（每次状态变化
  都会唤醒 worker 重新领取）。

### 6.3 等待提升（防饿死）

确定且可解释的规则：作业自可执行时刻（`not_before`）起，每完整等待
`JOB_AGING_THRESHOLD` 秒，有效优先级提升 1 档，最多提升
`JOB_AGING_MAX_BOOST` 档，且不超过 HIGH。等待足够久的低优先级作业
最终会与高优先级同档甚至更高，再按先提交先执行打破平局，因此
**不会被无限期饿死**。领取日志会记录 `aging_boost`，可核对提升轨迹。

### 6.4 排队原因（查询可解释）

`GET /jobs/{id}` 对等待中的作业返回 `queue_reason` 字段（现算，不落库）：

| queue_reason | 含义 |
|---|---|
| `QUEUED` | 等待空闲的全局执行名额 |
| `TYPE_QUOTA_FULL` | 所属类型的并发额度已占满 |
| `PRIORITY_WAIT` | 有更高（有效）优先级的到期作业排在前面 |
| `RETRY_BACKOFF` | 失败重试退避中，未到下次可执行时间 |

终态或执行中的作业该字段为 `null`。响应同时带 `priority` 与
`priority_name`。

### 6.5 与既有语义的兼容范围

- **领取原子性不变**：额度判定与状态翻转在同一个 SQLite IMMEDIATE
  事务内完成，同一作业不会被两个 worker 重复领取。
- **排队超时不变**：看门狗仍按 `queued_deadline <= now` 收口；即使作业
  是被类型额度挡住而等待，超时一样进入终态 `TIMED_OUT`。
- **取消不变**：可取消 `PENDING/RETRY_WAIT/RUNNING`，终态拒绝取消；
  取消释放的类型名额立即对等待作业可见。
- **重试不变**：退避中的作业（`RETRY_BACKOFF`）即使有空闲额度也不会
  被提前领取。
- **重启恢复不变**：遗留 `RUNNING` 置回 `PENDING` 后按原优先级重新参与
  调度；`priority` 列持久化，老库首次启动自动 `ALTER TABLE` 迁移并回填
  默认值 NORMAL。
- **幂等不变**：去重/冲突判定规则与之前完全一致。

---

## 7. HTTP 接口一览

| 方法 路径 | 说明 |
|---|---|
| `GET /` | 既有接口，返回 `{"Hello":"World"}`（行为不变） |
| `POST /jobs` | 提交作业；请求体见下，返回 `id/state/deduplicated` |
| `GET /jobs/{id}` | 查询作业（状态、输出、错误分类、每次尝试记录、优先级与排队原因） |
| `POST /jobs/{id}/cancel` | 取消非终态作业 |
| `GET /jobs?limit=` | 最近作业列表 |
| `GET /scheduler/quotas` | 查看类型并发额度与全局并发上限 |
| `PUT /scheduler/quotas` | 整体替换类型额度（`{"quotas":{"batch":2}}`），无需重启，仅影响新领取 |

`POST /jobs` 请求体：

```json
{
  "job_type": "flaky",
  "payload": {"fail_times": 2},
  "idempotency_key": "optional-unique-key",
  "max_attempts": 3,
  "run_timeout": 60.0,
  "queue_timeout": 300.0,
  "priority": 2
}
```

`priority` 可省略（默认 2=NORMAL），取值 1/2/3。

---

## 8. 本地验证

```bash
uv run pytest                      # 全部用例
uv run pytest -v --log-cli-level=INFO   # 逐条用例 + 完整作业/调度日志
```

测试日志会打印**作业标识、状态变化、尝试次数、错误分类与调度原因**
（优先级、有效优先级、等待提升档数、类型额度占用），例如：

```
job=<id> attempt=1 state=-->PENDING 作业已提交并入队 priority=3 priority_name=HIGH
job=<id> attempt=1 worker=0 领取原因=优先级调度 priority=3 effective_priority=3 aging_boost=0 type=rec type_quota=1/1
job=<id> attempt=1 worker=0 领取原因=等待提升+2档 priority=1 effective_priority=3 aging_boost=2 type=rec type_quota=unlimited
job=<id> attempt=1 state=RUNNING->RETRY_WAIT RETRYABLE：0.030s 后重试
job=<id> attempt=3 state=RUNNING->TIMED_OUT 执行超时且重试次数耗尽 error_code=ATTEMPT_TIMEOUT
job=<id> state=QUEUED->TIMED_OUT 排队超过 queue_timeout，进入终态 error_code=QUEUE_TIMEOUT
```

测试覆盖（`tests/`）：

- `test_scheduling.py`：同类型高优先级先领取、同档 FIFO、老请求缺省
  优先级兼容、类型额度隔离与 `TYPE_QUOTA_FULL` 原因、全局上限硬约束、
  运行中调整额度不重启且不打断在执行作业、取消释放类型名额、等待提升
  防饿死、`PRIORITY_WAIT`/`RETRY_BACKOFF` 排队原因、额度阻塞下排队超时
  仍生效、重启恢复保留优先级、HTTP 额度接口与非法额度 422。
- `test_concurrent_submit.py`：同一幂等键 20 并发提交只建一个作业；
  冲突载荷并发提交的去重/冲突结论；8 并发取消只有一个生效。
- `test_basic_and_idempotency.py`：提交成功、幂等去重、载荷/类型冲突、
  未知类型、404、HTTP 层幂等与 409、`GET /` 既有行为。
- `test_concurrency_cancel.py`：有界并发下排队不丢作业、队列满 `QUEUE_FULL`、
  取消排队中/执行中作业、终态拒绝取消、取消后状态不回退。
- `test_retry_timeout.py`：退避重试成功、重试耗尽 `FAILED`、不可重试立即失败、
  执行超时耗尽 `TIMED_OUT`、排队超时 `QUEUE_TIMEOUT`、退避有界确定性。
- `test_persistence.py`：终态结果重启可查、RUNNING 崩溃后恢复补跑、
  排队中作业重启后只执行一次。
- `test_error_classification.py`：`RETRYABLE / NON_RETRYABLE / TIMEOUT`
  在终态与每次尝试记录中一致可区分；四类终态互不相同；HTTP 错误码可区分。

### 手动验证调度行为

```bash
# 启动时给 batch 类型 1 个并发额度
JOB_TYPE_QUOTAS='{"batch":1}' uv run uvicorn main:app --port 8000

# 提交两个 batch 长作业 + 一个 echo：echo 不会被 batch 堵住
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"job_type":"sleep","payload":{"seconds":30}}'
# 查询等待中的同类作业，可见 "queue_reason":"TYPE_QUOTA_FULL"

# 不重启在线调大批量额度，等待中的 batch 作业随即被领取
curl -s -X PUT localhost:8000/scheduler/quotas -H 'Content-Type: application/json' \
  -d '{"quotas":{"batch":2}}'
```

### 手动验证重启持久化

```bash
JOB_DB_PATH=/tmp/demo.db uv run uvicorn main:app --port 8000
# 提交一个长作业后 Ctrl+C 停止，再用同一 JOB_DB_PATH 启动；
# 已完成作业结果仍可 GET 到，未完成作业会被恢复并继续调度。
```

---

## 9. 代码结构

```
app/
  config.py     # Settings：并发/排队/超时/重试/调度配置（环境变量）
  errors.py     # ErrorCode 与可区分的业务异常
  models.py     # 状态机枚举、优先级档位、排队原因、JobRecord/请求响应模型
  storage.py    # SQLite 存储：唯一幂等键、额度感知原子领取、CAS、重启恢复、排队超时
  handlers.py   # 内置作业类型与处理器注册、可取消的 ctx.sleep
  manager.py    # 运行时核心：worker 池 + 看门狗 + 提交/查询/取消/重试/调度
  api.py        # FastAPI 路由（含 /scheduler/quotas）与异常->HTTP 映射
main.py         # create_app 装配与 lifespan（建表、迁移、启动恢复）
tests/          # pytest / pytest-asyncio 用例
```
