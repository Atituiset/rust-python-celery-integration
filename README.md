# Rust + Python Celery + Redis 最小闭环实现

基于 [`docs/arch-report.html`](docs/arch-report.html) 架构报告的**最小可运行闭环**实现。Rust 端作为 Producer 推送任务，Python Celery Worker 消费执行，结果通过 Redis 回传，Rust 端轮询获取。

## 架构概览

```
┌──────────────────┐          LPUSH           ┌──────────────┐
│                  │ ───────────────────────▶ │              │
│  rust-producer   │     Redis DB 6 (Broker)  │   Redis 7    │
│  (Task Producer) │                          │   Middleware │
│                  │ ◀─────────────────────── │              │
└──────────────────┘         GET/轮询          └──────────────┘
                                                       │
                                              BRPOP    │    SET
                                                │      │
                                                ▼      ▼
                                       ┌────────────────────┐
                                       │  python-worker     │
                                       │  (Celery Consumer) │
                                       └────────────────────┘
```

| 组件 | 技术栈 | 职责 |
|------|--------|------|
| `rust-producer` | Rust, tokio, redis-rs | 构造 Celery v2 消息体，LPUSH 到 Redis DB 6；轮询 DB 7 获取结果 |
| `python-worker` | Python, Celery 5.x, kombu | BRPOP 消费任务，执行扫描逻辑，结果自动写回 DB 7 |
| `redis` | Redis 7 | DB 6 = Broker (任务队列), DB 7 = Backend (结果存储) |

## 项目结构

```
├── docker-compose.yml          # Redis 服务编排
├── docs/
│   └── arch-report.html        # 原始架构分析报告（只读）
├── python-worker/
│   ├── .venv/                  # uv 虚拟环境
│   ├── requirements.txt        # celery[redis], redis
│   ├── celeryconfig.py         # broker_url=DB6, result_backend=DB7
│   └── tasks.py                # @task(name="scan.task")
├── rust-producer/
│   ├── Cargo.toml              # tokio, redis, serde, uuid, base64
│   └── src/main.rs             # push_task() + poll_result()
└── README.md                   # 本文件
```

## 快速开始

### 1. 启动 Redis

```bash
# 方式一：系统 Redis（已安装时）
redis-server --daemonize yes --port 6379

# 方式二：Docker
docker compose up -d redis
```

### 2. 启动 Python Worker

```bash
cd python-worker

# 创建虚拟环境（首次）
uv venv
uv pip install -r requirements.txt

# 启动 Worker
.venv/bin/celery -A tasks worker --loglevel=info
```

Worker 启动后应显示：
```
[tasks]
  . scan.task

Connected to redis://localhost:6379/6
```

### 3. 运行 Rust Producer

```bash
cd rust-producer
cargo run
```

### 4. 预期输出

```
[Producer] Connected to Redis
[Producer] Task pushed: id=4110003a-336d-4b97-b945-79e304299e97, task=scan.task
[Poller] Waiting for result...
[Poller] Status: STARTED
[Poller] Status: SUCCESS

========== RESULT ==========
Status : SUCCESS
Task ID: 4110003a-336d-4b97-b945-79e304299e97
Result : {
  "findings_count": 3,
  "repo_path": "/path/to/repo",
  "scanned_files": ["file1.c", "file2.c", "main.c"],
  "status": "ok"
}
============================
```

## 实现细节

### Celery v2 消息体格式（Rust 端手动构造）

Celery Worker 通过 kombu 消费 Redis LIST，消息需符合以下结构：

```json
{
  "body": "base64([args, kwargs, embed])",
  "content-encoding": "utf-8",
  "content-type": "application/json",
  "headers": {
    "lang": "py",
    "task": "scan.task",
    "id": "<uuid>",
    "root_id": "<uuid>",
    "parent_id": null,
    "group": null,
    "meth": null,
    "shadow": null,
    "eta": null,
    "expires": null,
    "retries": 0,
    "timelimit": [null, null],
    "argsrepr": "...",
    "kwargsrepr": "{}",
    "origin": "rust-producer"
  },
  "properties": {
    "correlation_id": "<uuid>",
    "reply_to": "<uuid>",
    "delivery_mode": 2,
    "delivery_info": { "exchange": "", "routing_key": "celery" },
    "priority": 0,
    "body_encoding": "base64",
    "delivery_tag": "<uuid>"   // kombu 解析必需字段
  }
}
```

### Worker 消费机制

Celery Worker 启动后对 Redis 执行 `BRPOP celery`，阻塞等待消息。Rust `LPUSH` 后 Worker 立即收到消息，根据 `headers.task` 路由到注册表中的 `scan.task` 函数执行。执行完成后 Celery 自动将结果 `SET` 到 `celery-task-meta-<task_id>`。

### 轮询策略

Rust 端当前采用 500ms 间隔的主动轮询（`GET celery-task-meta-<id>`）。生产环境可升级为 Redis **Keyspace Notifications** + Pub/Sub，实现事件驱动式结果获取。

## 踩坑记录

| 问题 | 原因 | 解决 |
|------|------|------|
| Worker `KeyError: 'delivery_tag'` | `properties` 缺少 `delivery_tag` | 添加 `delivery_tag: <uuid>` |
| `lpush` 编译警告 | Rust 2024 edition 类型推断变化 | 显式标注 `lpush::<_, _, ()>` |
| Worker 未消费 | 消息格式与 kombu 不兼容 | 确保 body 为 base64 编码的 `[args, kwargs, embed]` 三元组 |

## 后续演进

- [ ] 轮询升级为 Redis Keyspace Notifications + Pub/Sub
- [ ] 接入真实 AST 扫描逻辑（Rust 端）和安全编译选项分析（Python 端）

## License

MIT
