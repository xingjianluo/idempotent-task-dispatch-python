# 本地幂等任务投递与消费（Python）

一个基于 SQLite + 线程池的**单进程本地任务队列**，把下面四件事讲清楚：

1. **任务投递与幂等**：同一业务键重复投递不会重复执行；键被占用、内容冲突、请求非法是三种可区分的拒绝。
2. **分组顺序消费与失败重试**：同组同键串行、异键并发；失败/超时按指数退避重试，重试用完进死信并留下可查原因。
3. **并发与重启下状态一致**：投递、消费、重试、进死信同时发生时数量守恒，任务不会“既没完成也没进死信”；重启后已确认的任务不再执行。
4. **可观测**：关键动作都打日志，含分组、任务键、`attempts/max_attempts` 和判定理由 `reason=`。

## 目录结构

```
taskdispatch/
  exceptions.py   # InvalidRequest / KeyOccupied / PayloadConflict 三类拒绝
  models.py       # TaskState 状态机、TaskRecord、DispatchOutcome
  store.py        # SQLite 存储：短事务 + CAS 状态流转 + 租约回收
  service.py      # 投递服务：校验、幂等判定、冲突区分、退避函数
  worker.py       # 分组消费者：同键串行/异键并发、租约超时、重试、死信、重启恢复
  app.py          # FastAPI：POST /tasks、GET /groups/{group}、GET /health
main.py           # uvicorn 入口
tests/            # 幂等 / 重试死信 / 并发重启 / HTTP / 日志 五类测试
```

## 快速开始

```bash
uv sync --group dev          # 安装依赖（含 pytest）
python -m pytest -q          # 跑全部测试
python main.py               # 启动 HTTP 服务（默认 127.0.0.1:8000）
```

### HTTP 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /tasks` | 投递任务。body：`{"group","key","payload","max_attempts"?}` |
| `GET /groups/{group}` | 该分组各状态计数 + 死信列表（含失败原因、尝试次数） |
| `GET /health` | 全局各状态计数 |

```bash
# 新建 -> 201
curl -s -X POST localhost:8000/tasks \
  -H 'content-type: application/json' \
  -d '{"group":"orders","key":"order-1","payload":{"amount":100}}'
# 同键同内容、任务在途 -> 409 {"reason":"key_occupied"}
# 同键不同内容          -> 409 {"reason":"payload_conflict"}
# 参数非法              -> 400 {"reason":"invalid_request"}
# 同键同内容、已终态     -> 200 {"duplicated":true,...}（不重复执行）
```

编程方式使用见 `Worker` / `TaskService`：

```python
from taskdispatch.service import TaskService
from taskdispatch.store import TaskStore
from taskdispatch.worker import Worker

store = TaskStore("tasks.db")                 # 文件库：重启后状态仍在
service = TaskService(store, max_attempts=3)  # 默认指数退避 1s/2s/4s...
service.dispatch("orders", "order-1", {"amount": 100})

worker = Worker(store, "orders", handler=lambda rec: "processed", concurrency=8)
worker.start()          # start 时自动 recover(force_all=True)，收回上次崩溃遗留的 RUNNING
```

## 1. 幂等的边界

幂等键是 **`(group, key)`** 二元组，库内有 `UNIQUE(grp, key)` 约束。载荷通过
`json.dumps(..., sort_keys=True)` 规范化后取 SHA-256 做内容指纹。投递判定顺序：

| 情况 | 结果 | HTTP |
| --- | --- | --- |
| `group`/`key` 为空、`payload` 为 null、`max_attempts<1`、载荷不可序列化 | `InvalidRequestError` | 400 |
| 键不存在 | 新建 `PENDING` | 201 |
| 键存在，**载荷指纹不同** | `PayloadConflictError`（内容冲突） | 409 `reason=payload_conflict` |
| 键存在，载荷相同，任务在途（`PENDING`/`RUNNING`） | `KeyOccupiedError`（重复投递，键被占用） | 409 `reason=key_occupied` |
| 键存在，载荷相同，已终态（`SUCCEEDED`/`DEAD`） | 幂等返回已有任务，**不重新执行** | 200 `duplicated=true` |

边界说明：

- **幂等只覆盖“投递”这一步**：它保证同一键的任务只存在一份、只被消费者处理到终态一次；
  但消费者是**至少一次（at-least-once）**语义——若进程在“业务已做完”与“写回
  SUCCEEDED”之间崩溃，重启后会再执行一次 handler。**handler 必须自身幂等**
  （用业务键做去重/upsert）。
- JSON 对象的字段顺序不影响指纹；数字、大小写、空白是有意义的内容差异。
- 死信（`DEAD`）也是终态：同内容重放返回 `duplicated`，不同内容依旧报冲突；
  要让死信重跑，需要人工介入（系统不自动复活死信）。
- 幂等范围是**单个库文件/单个进程**；多进程共享同一文件库未做行级抢占，
  生产部署应只让一个 Worker 进程消费同一 `group`。

## 2. 重试与死信规则

- 每个任务有 `attempts`（已尝试次数）和 `max_attempts`（默认 3，投递时可覆盖）。
- 一次尝试 = 领取任务（`PENDING -> RUNNING, attempts+1`）后执行 handler。
- handler **抛异常**或**执行超过租约 `lease_seconds`** 都算失败：
  - `attempts < max_attempts`：写回 `PENDING`，`available_at = now + backoff(attempts)`，
    默认退避 `min(60s, 2**(attempts-1))`（1s, 2s, 4s, …），可注入自定义函数（测试里常设为 0）。
  - `attempts == max_attempts`：`RUNNING -> DEAD`，`last_error` 写入
    `"retries exhausted after N attempts: <最后一次异常类型与消息>"`。
- 失败原因随时可查：`TaskRecord.last_error` / `GET /groups/{group}` 的 `dead[].last_error`。
- 超时后旧 handler 线程不会被杀掉（Python 无法安全强杀线程）。在该线程真正结束前，
  任务**不允许被再次领取**，避免同一份业务逻辑并发跑两遍、也避免 attempts 被推过上限；
  旧线程迟到的完成/重试/写死信会因 CAS（`attempts` 不匹配）失效，不会重复生效。

## 3. 并发与重启约定

- **同组同键串行**：同一键同一时刻至多一个 `RUNNING`，因此“每单一个任务”时天然按投递顺序处理；
  **不同键并发**，并发度由 `concurrency` 控制。
- **状态机守恒**：任务在任意时刻恰好处于 `PENDING / RUNNING / SUCCEEDED / DEAD` 之一，
  每次流转都是一条带 WHERE 条件的原子 UPDATE（短事务 `BEGIN IMMEDIATE`），
  所以 `total = PENDING+RUNNING+SUCCEEDED+DEAD` 恒成立，不存在丢失中间态。
- **投递原子性**：“查已有任务 → 判定 → 插入”全程持进程内排他锁，并发同键投递
  只会有一个成功，其余确定性地落到“占用/冲突”，不会撞唯一键报 500。
- **租约与恢复**：
  - 运行中 Worker 周期性把 `leased_at < now-lease` 的 `RUNNING` 收回 `PENDING`（保留 attempts）；
  - `start()` 时执行 `recover(force_all=True)`：新进程不信任旧租约时钟，
    把本分组全部遗留 `RUNNING` 收回（崩溃恢复）；
  - 已 `SUCCEEDED`/`DEAD` 的任务不受恢复影响，**重启后不会再跑**。
- 存储默认 `:memory:`（测试用）；传入文件路径时启用 WAL，已 COMMIT 的状态在进程重启后仍在。

## 4. 日志约定

所有关键动作由 `taskdispatch.service` / `taskdispatch.worker` 两个 logger 输出，
统一带 `group=`、`key=`、`task_id=`、`attempts=N/M` 和判定理由，例如：

```
dispatch accepted group=orders key=order-1 task_id=1 reason=new
dispatch rejected group=orders key=order-1 task_id=1 attempts=0 state=PENDING reason=key_occupied
task retry  group=orders key=order-7 task_id=7 attempts=1/3 applied=True retry_in=1.000 reason=handler_failed last_error='RuntimeError: boom'
task timeout group=orders key=order-7 task_id=7 attempts=1/3 reason=lease_timeout lease=30.0s
task dead   group=orders key=order-7 task_id=7 attempts=3/3 applied=True reason=retries_exhausted last_error='RuntimeError: boom'
task finished group=orders key=order-1 task_id=1 attempts=1/3 applied=True reason=succeeded
worker recover group=orders reclaimed=2 reason=restart
```

`applied=False` 表示这是一次**陈旧尝试**的迟到写回（任务已被超时收回并重新领取），
状态机安全地忽略了它——在排查“为什么重试了”时可据此区分。

## 5. 怎么验证

```bash
python -m pytest -q                       # 全部用例
python -m pytest tests/test_concurrency.py -q   # 并发投递 / 异键并发 / 混合负载守恒 / 重启恢复
python -m pytest tests/test_retry_deadletter.py # 退避延迟 / 重试耗尽进死信 / 死信不复活
```

测试覆盖与对应断言：

- **并发投递**：32 线程同键同内容 → 恰好 1 个新建、其余 31 个 `key_occupied`，总数仍为 1；
  24 线程同键不同内容 → 1 新建 + 23 个 `payload_conflict`，两类拒绝互不混淆。
- **重复投递**：在途重复 → 占用；终态后同内容 → `duplicated` 且 task_id 不变、不产生新任务。
- **重试耗尽进死信**：断言 `attempts == max_attempts`、状态为 `DEAD`、
  `last_error` 同时包含“retries exhausted”与最后一次异常消息，死信列表可查。
- **数量守恒**：投递与消费并发进行时，任意时刻
  `PENDING+RUNNING+SUCCEEDED+DEAD == 累计接受数`，结束时 `PENDING=RUNNING=0`。
- **重启恢复**：文件库重开后，遗留 `RUNNING` 被收回重跑（attempts 保留），
  已 `SUCCEEDED` 的任务 handler 绝不被再次调用。
- **日志**：断言投递/重试/死信/超时日志中都含 `key=`、`attempts=N/M` 与对应 `reason=`。

并发用例不依赖固定 sleep 做判定（用栅栏同步），整套测试经多轮重复执行验证稳定。
