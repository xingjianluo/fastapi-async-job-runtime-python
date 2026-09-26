# FastAPI 本地异步作业运行时

在原有 FastAPI 服务（`GET /` 行为保持不变）之上，新增一套**完全本地、无外部服务
依赖**的异步作业运行时：提交 / 查询 / 取消、幂等去重、有界并发与排队、排队与执行
超时、失败分类与有限退避重试，以及基于 SQLite 的重启持久化。

- 存储：Python 标准库 `sqlite3`（默认文件 `./job_runtime.db`，无需安装任何数据库）
- 并发：`asyncio` 单 dispatcher + 有界 worker 槽位
- 对外：FastAPI HTTP 接口，错误码明确可区分
- 文档：状态机与一致性规则见 [`docs/state-machine.md`](docs/state-machine.md)

## 快速开始

```bash
uv sync
uv run uvicorn main:app --port 8000
```

提交一个作业：

```bash
curl -s -X POST http://127.0.0.1:8000/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-42' \
  -d '{"job_type": "succeed", "payload": {"hello": "world"}}'
```

查询：

```bash
curl -s http://127.0.0.1:8000/jobs/<job_id>
```

取消：

```bash
curl -s -X POST http://127.0.0.1:8000/jobs/<job_id>/cancel
```

## HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /jobs` | 提交作业；可带 `Idempotency-Key` 头或请求体字段 |
| `GET /jobs/{id}` | 查询状态、结果、错误分类与每次尝试的历史 |
| `POST /jobs/{id}/cancel` | 取消非终态作业 |
| `GET /` | 原有接口，保持返回 `{"Hello": "World"}` |

提交请求体：

```json
{
  "job_type": "flaky",
  "payload": {"key": "a", "fail_times": 2},
  "idempotency_key": "optional-key",
  "max_attempts": 3
}
```

响应（`reused=true` 表示命中幂等复用）：

```json
{
  "reused": false,
  "job": {
    "id": "...", "state": "QUEUED", "attempt": 0, "max_attempts": 3,
    "job_type": "flaky", "result": null,
    "error_class": "NONE", "error_message": null,
    "attempt_history": [], "...": "..."
  }
}
```

错误响应统一为 `{"error": {"code", "message"}}`：

| code | HTTP | 含义 |
| --- | --- | --- |
| `IDEMPOTENCY_PAYLOAD_CONFLICT` | 409 | 同幂等键但载荷（或类型）不一致 |
| `TERMINAL_STATE_CONFLICT` | 409 | 对已终态作业重复提交 / 取消 |
| `JOB_NOT_FOUND` | 404 | 作业不存在 |
| `UNKNOWN_JOB_TYPE` | 422 | 提交了未注册的 job_type |
| `QUEUE_FULL` | 503 | 有界队列持续饱和 |

## 内置作业类型（均为本地、无外部依赖）

| job_type | 行为 |
| --- | --- |
| `succeed` | 可选 `delay` 秒后返回 `{"echo": payload}` |
| `fail_retryable` | 始终抛可重试错误 |
| `fail_permanent` | 始终抛不可重试错误 |
| `flaky` | 前 `fail_times` 次可重试失败，之后成功 |
| `slow` | 睡眠 `seconds` 秒，用于验证执行超时 / 取消 |
| `boom` | 抛出未分类异常，验证按不可重试处理 |

### 注册自定义处理器

```python
from job_runtime.handlers import HandlerRegistry, JobExecutionError

async def charge_order(job_id: str, payload: dict):
    ok = payload.get("ok", True)
    if not ok:
        raise JobExecutionError("card declined", retryable=False)
    return {"charged": payload["amount"]}

registry = HandlerRegistry()
registry.register("charge", charge_order)
# 然后：JobService(config, registry=registry)
```

处理器抛出：

- `JobExecutionError(msg, retryable=True)`  → 分类 `RETRYABLE`，按有限次数退避重试；
- `JobExecutionError(msg, retryable=False)` → 分类 `NON_RETRYABLE`，立即失败；
- 其他任意异常 → 分类 `NON_RETRYABLE`（异常类型写入 error_message），立即失败。

## 配置项

可用构造参数或环境变量（前缀 `JOB_RUNTIME_*`）覆盖：

| 配置 | 环境变量 | 默认 | 说明 |
| --- | --- | --- | --- |
| `max_concurrency` | `JOB_RUNTIME_MAX_CONCURRENCY` | 4 | 同时执行的作业上限 |
| `max_queue_size` | `JOB_RUNTIME_MAX_QUEUE_SIZE` | 1000 | QUEUED 队列容量，超出则短暂等待，持续饱和返回 503 |
| `attempt_timeout` | `JOB_RUNTIME_ATTEMPT_TIMEOUT` | 30.0 | 单次尝试执行超时（秒） |
| `queue_timeout` | `JOB_RUNTIME_QUEUE_TIMEOUT` | 600.0 | 排队最长等待（秒，以单个进程的运行时钟计；重启后重新计时） |
| `default_max_attempts` | `JOB_RUNTIME_DEFAULT_MAX_ATTEMPTS` | 3 | 默认最大尝试次数（含首次） |
| `backoff_base_delay` | `JOB_RUNTIME_BACKOFF_BASE` | 0.2 | 第 2 次尝试前的基础退避（秒） |
| `backoff_factor` | `JOB_RUNTIME_BACKOFF_FACTOR` | 2.0 | 退避倍率 |
| `backoff_max_delay` | `JOB_RUNTIME_BACKOFF_MAX` | 30.0 | 单次退避上限（秒） |
| `poll_interval` | `JOB_RUNTIME_POLL_INTERVAL` | 0.05 | dispatcher 空轮询间隔（秒） |
| `db_path` | `JOB_RUNTIME_DB` | `job_runtime.db` | SQLite 文件；`:memory:` 为内存库 |

退避公式：`delay(n) = min(backoff_base_delay * backoff_factor^(n-2), backoff_max_delay)`。

## 本地验证

```bash
# 全部 33 个测试（并发提交、幂等冲突、取消、排队/执行超时、
# 重试耗尽、重启恢复、HTTP 行为）
uv run pytest -s

# 只看状态流转日志（日志中包含作业标识、状态变化、错误分类）
uv run pytest tests/test_retry.py -s
```

也可以运行一个手工演示脚本（向真实 HTTP 服务提交各内置类型作业）：

```bash
uv run python scripts/demo.py
```

持久化验证：先 `uv run uvicorn main:app` 提交作业并等待完成，`Ctrl+C` 停止后
重新启动，`GET /jobs/{id}` 仍返回终态结果；未完成的 QUEUED / AWAITING_RETRY
作业会在新进程中继续。有序关停时仍在执行的作业被标记为
`CANCELLED`（服务关停主动中止）；若进程是崩溃（未执行关停流程），重启后停留在
RUNNING 的作业会被明确标记为 `FAILED(INTERRUPTED)`，绝不重复执行。

## 代码结构

```
main.py                     # FastAPI 入口，保留 GET /，挂载 /jobs
job_runtime/
  models.py                 # 状态/错误枚举、Job 数据模型、API 模型、异常
  config.py                 # RuntimeConfig（环境变量可覆盖）
  storage.py                # sqlite3 持久化与 CAS 状态转移、启动恢复
  handlers.py               # 处理器注册表、失败契约、内置演示处理器
  runner.py                 # 有界并发调度器：排队、超时、退避重试、取消
  service.py                # 门面：提交/查询/取消 + 幂等与容量规则
  api.py                    # /jobs 路由与错误处理
  log.py                    # 统一日志（作业标识、状态变化、错误分类）
tests/                      # pytest 测试套件
docs/state-machine.md       # 状态机与一致性规则
scripts/demo.py             # 本地 HTTP 演示
```
