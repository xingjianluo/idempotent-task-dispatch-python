# 本地幂等任务投递与消费

一个进程内的本地任务系统：基于 FastAPI 暴露投递接口，用 SQLite（WAL）做持久化，
提供**幂等投递、分组有序消费、指数退避重试、死信**，并保证并发与重启下的数量守恒。

## 运行

```bash
uv run python main.py            # 默认使用同目录 tasks.db
TASK_DB_PATH=/tmp/t.db uv run python main.py
uv run pytest -q                 # 运行全部测试（20 个）
```

投递一个任务：

```bash
curl -s -X POST localhost:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"business_key":"order-123","group_name":"orders","payload":"{\"amount\":10}"}'
```

## 概念与状态

任务状态只有四种，且每个业务键（`business_key`）在表里**全局唯一**：

```
pending ──认领──▶ processing ──成功────────▶ succeeded
                     │
                     ├──失败且还有次数──▶ pending（带 visible_after 退避）
                     └──失败且次数耗尽──▶ dead
```

- `pending`：等待消费（包含退避中的任务，以 `visible_after` 控制可见时间）。
- `processing`：已被某个 worker 认领，正在执行。
- `succeeded` / `dead`：终态，永远不会再被执行。

数量守恒：`pending + processing + succeeded + dead = 总投递去重数`，
任何时刻都成立；不会出现任务既没成功也没进死信的悬挂情况。
可通过 `GET /counters/summary` 随时核对。

## 一、幂等的边界

幂等的判定单位是 **业务键 + 载荷哈希（payload 的 SHA-256）**。重复投递按当前状态
和载荷是否一致被**区分**处理，而不是一律报同一个错：

| 当前任务状态 | 载荷相同 | 载荷不同 |
| --- | --- | --- |
| 不存在 | `accepted`（201，新建） | — |
| `pending` / `processing` | `duplicate_in_flight`（200，幂等返回已有任务，不重复执行） | **`key_occupied`（409）** 键正被占用 |
| `succeeded` | `replay_succeeded`（200，返回终态，绝不重跑） | **`content_conflict`（422）** 终态内容冲突 |
| `dead` | `replay_dead`（200，返回死信任务，不重跑） | **`content_conflict`（422）** 终态内容冲突 |

参数本身不合法（空键/空分组、键超长、载荷超过 64KiB、类型错误）返回
**`invalid_request`（400）**。

要点：

- **只做"去重"，不做"覆盖"**：同一个业务键一旦用过就不能再携带不同内容，
  无论原任务还在跑还是已结束；两种情况用 409/422 区分，调用方可以据此决定
  是"稍后重试"还是"必须换新键"。
- 终态任务（成功或死信）即使以相同载荷重复投递，也**不会再执行一次**，
  这是重启后也成立的保证（终态记录持久化在 SQLite）。
- 投递判定（先查后插）在一个 `BEGIN IMMEDIATE` 事务内完成，因此 50 个并发线程
  用同一个键投递，也恰好只有 1 个被受理。

边界（本实现明确**不**提供的语义）：

- 幂等键是全局唯一的，不按分组划分；同一业务键不能同时出现在两个分组。
- 载荷以原始字符串整体哈希，不做字段级合并或部分更新。
- 死信不会自动重放；相同键+相同载荷只会返回死信记录，需要人工介入或换新键。

## 二、分组消费、重试与死信规则

### 分组与顺序

- worker 认领任务时，认领的是某个 `(group, business_key)` 队首：**同组同键严格
  按 `id`（即投递顺序）处理；不同业务键互不阻塞，可被多个 worker 并发处理。**
- 一个正在退避中的键会"挡住"它后面同键的任务——在 `visible_after` 到期之前，
  该键不会被认领（认领 SQL 用 `NOT EXISTS 更早的 pending 同键任务` 保证）。
- 分组本身不限并发；并发度由 `workers` 控制，跨组、跨键都可以并行。

> 由于业务键全局唯一，同一时刻一个键至多对应一行任务；"同键顺序"主要体现在
> 该键多次失败重试时，后一次尝试不能越过退避提前执行。

### 重试退避

由 `RetryPolicy` 配置：

- `max_attempts`：总尝试次数（含首次），默认 3。
- 第 n 次尝试失败后的延迟：
  `delay = min(max_delay, base_delay * factor ** (n - 1))`，
  默认 `base_delay=1s, factor=2, max_delay=30s`（即 1s、2s、4s……封顶 30s）。
- `timeout`：单次尝试的墙钟超时，默认 5s；`None` 表示不限制。
- 处理函数**抛异常**或**超时**都算一次失败尝试。
  超时后任务继续按退避/死信推进；超时的处理线程无法强杀，其迟到结果会被丢弃，
  因此状态转移以数据库为准，不会重复提交结果（at-most-once 提交语义）。

### 死信

- 当失败尝试次数达到 `max_attempts`，任务转入 `dead`，不再被调度。
- `last_error` 保留最后一次失败原因（异常类名 + 信息，或超时描述），
  `attempts` 记录实际尝试次数。
- 查询：`GET /dead-letters` 列出全部死信及失败原因；`GET /tasks/{business_key}`
  查单个任务状态。

## 三、并发与重启约定

- **单连接 + 锁**：所有数据库访问经一个 SQLite 连接（WAL）和一把进程内锁串行化，
  认领、成功、重试、进死信都是带状态条件的原子 `UPDATE`
  （例如 `WHERE id=? AND state='processing'`），状态不符时更新 0 行并记录日志，
  避免丢失更新。
- **认领即落盘**：认领时立刻把 `pending` 改成 `processing`；处理结果只决定
  后续走向，崩溃最多留下一条 `processing`，不会丢任务。
- **重启恢复**：dispatcher 启动时会把上次进程残留的 `processing` 行重新置为
  `pending` 立即重投（决策日志记为 `restart_requeue`）。因此：
  - 已 `succeeded` / `dead` 的任务重启后不会重跑；
  - 崩溃在处理中途的任务会被重新执行一次（无外部结果的 at-least-once 投递、
    at-most-once 提交），需要严格恰好一次的副作用应由业务侧用业务键自行去重。

## 四、日志

所有任务生命周期日志都带三个结构化字段，便于检索：

```
... task failed, scheduling retry ... task_key=order-123 attempt=2 decision=retry
... task exhausted retries ...       task_key=order-123 attempt=3 decision=dead
... dispatch judged ...              task_key=order-123 attempt=0 decision=accepted
```

`decision` 取值：`accepted / duplicate_in_flight / replay_succeeded / replay_dead /
key_occupied / content_conflict / invalid_request / claimed / retry / succeeded /
dead / restart_requeue / state_changed`。

## 五、HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /tasks` | 投递；body：`business_key, group_name?, payload?`。201 新建 / 200 幂等重放 / 400 / 409 / 422 |
| `GET /tasks/{business_key}` | 查询单个任务（含状态、尝试次数、`last_error`） |
| `GET /counters/summary` | 四种状态计数与总数，用于核对守恒 |
| `GET /dead-letters` | 全部死信及失败原因 |

错误响应体统一形如：

```json
{"detail": {"reason": "key_occupied", "message": "...", "existing_task_id": 7}}
```

## 六、如何验证

```bash
uv run pytest -q
```

覆盖三类重点场景（见 `tests/`）：

1. **并发投递**（`test_concurrency.py`）：50 线程同键投递恰好 1 个受理；
   32 线程不同内容抢键，1 个受理 + 31 个 `key_occupied`，不产生多余行；
   100 个键 8 worker 并发消费后数量守恒、每个任务恰好执行一次。
2. **重复投递**（`test_idempotency.py`）：`invalid_request`、`key_occupied`、
   `content_conflict` 三类拒绝原因分别断言；处理中重复返回
   `duplicate_in_flight`；成功/死信后重复投递不重跑。
3. **重试耗尽进死信**（`test_retry_deadletter.py`）：尝试次数正确、退避真实
   延迟、超时算失败、耗尽后进 `dead` 且 `last_error` 可查、全程计数守恒。
4. **重启**：成功任务重启不重跑；模拟崩溃留下的 `processing` 孤儿任务在新
   dispatcher 启动后被重投并恰好完成一次。
5. **日志**（`test_logging.py`）：断言重试/死信/认领/成功日志均带
   `task_key`、`attempt`、`decision`。

仓库还可手动进行压力验证（脚本方式）：1200 次并发投递归并为 300 个唯一键，
其中 40 个恒失败 → 最终 `succeeded=260, dead=40, pending=processing=0`。

## 目录结构

```
idempotent_tasks/
  models.py         # 状态、拒绝原因、RetryPolicy、值对象
  errors.py         # 三类可区分异常
  store.py          # SQLite 存储与原子状态转移
  dispatcher.py     # 投递幂等判定、worker 循环、重试/死信决策
  logging_setup.py  # 结构化日志
  api.py            # FastAPI 应用
main.py             # 装配入口
tests/              # 幂等 / 重试死信 / 并发重启 / 日志 测试
```
