# Rust + Python Celery + Redis 最小闭环实现

基于 [`docs/arch-report.html`](docs/arch-report.html) 架构报告的**最小可运行闭环**实现。Rust 端作为 Producer 推送任务，Python Celery Worker 消费执行，结果通过 Redis **Keyspace Notifications + Pub/Sub 事件驱动**回传，零轮询、零无效 Redis 请求。

## 架构概览

```
┌──────────────────┐          LPUSH           ┌──────────────┐
│                  │ ───────────────────────▶ │              │
│  rust-producer   │     Redis DB 6 (Broker)  │   Redis 7    │
│  (Task Producer) │                          │   Middleware │
│                  │ ◀────Pub/Sub 事件─────── │              │
└──────────────────┘   (Keyspace Notifications)└──────────────┘
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
| `celery-redis-producer` | Rust, tokio, redis-rs | 可复用 Crate：构造 Celery v2 消息体 LPUSH 到 DB 6；单一 Pub/Sub 长连接订阅 DB 7 `set` 事件，通过 `oneshot` 内存通道精确唤醒任务协程 |
| `rust-producer` | Rust (示例应用) | 演示如何使用 `celery-redis-producer` crate |
| `python-worker` | Python, Celery 5.x, kombu | BRPOP 消费任务，执行扫描逻辑，结果自动写回 DB 7 |
| `redis` | Redis 7 | DB 6 = Broker (任务队列), DB 7 = Backend (结果存储); Keyspace Notifications 推送结果事件 |

## 项目结构

```
├── .env                        # Redis 连接配置 (broker / backend URL)
├── docker-compose.yml          # Redis 服务编排
├── docs/
│   └── arch-report.html        # 原始架构分析报告（只读）
├── python-worker/
│   ├── .venv/                  # uv 虚拟环境
│   ├── requirements.txt        # celery[redis], redis, python-dotenv
│   ├── celeryconfig.py         # 从 .env 读取 broker_url / result_backend
│   └── tasks.py                # @task(name="scan.task")
├── rust-celery-producer/       # 可复用 Rust Crate（零第三方 Celery 依赖）
│   ├── Cargo.toml              # celery-redis-producer v0.1.0
│   └── src/
│       ├── lib.rs              # 公开 API: Producer, ResultListener, CeleryResult
│       ├── protocol.rs         # Celery v2 消息体强类型定义
│       ├── producer.rs         # Producer: 构造消息并 LPUSH 到 Redis
│       └── result.rs           # ResultListener: Keyspace Pub/Sub + oneshot 事件总线
├── rust-producer/              # 示例应用（依赖上方 crate）
│   ├── Cargo.toml              # 引入 celery-redis-producer
│   └── src/main.rs             # 示例: 推送任务并等待结果
└── README.md                   # 本文件
```

## 配置

Redis 连接地址通过项目根目录的 `.env` 文件管理，Rust 和 Python 两侧共享同一配置：

```bash
# .env
REDIS_BROKER_URL=redis://localhost:6379/6    # Celery broker (任务队列)
REDIS_BACKEND_URL=redis://localhost:6379/7   # Celery backend (结果存储)
```

两侧代码均优先读取环境变量，若未设置则回退到上述默认值。

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

# Linux / macOS / WSL
.venv/bin/celery -A tasks worker --loglevel=info

# Windows (原生)
.venv\Scripts\celery -A tasks worker --loglevel=info
```

Worker 启动后应显示：
```
[tasks]
  . scan.task

Connected to redis://localhost:6379/6   # 实际 URL 以 .env 配置为准
```

### 3. 运行 Rust Producer

```bash
cd rust-producer
cargo run
```

### 4. 预期输出

```
[Producer] Connected to Redis
[Listener] Global Redis Keyspace listener started
[Producer] Task pushed: id=8b2cb071-1283-4665-9566-8278dada1452, task=scan.task
[Listener] Waiting for result...

========== RESULT ==========
Status : SUCCESS
Task ID: 8b2cb071-1283-4665-9566-8278dada1452
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

### 结果获取策略：全局事件总线

Rust 端维护**单一** Redis Pub/Sub 长连接，订阅 `__keyevent@7__:set`。当 Celery Worker 将结果写入 DB 7 时，Redis 自动推送事件，监听器通过 `tokio::sync::oneshot` 内存通道精确唤醒对应任务协程。

**防竞态双重检查：**
1. 注册 oneshot 前先 `GET` 一次（任务可能已完成）
2. 注册后再 `GET` 一次（消灭注册窗口期竞态）
3. 被唤醒后检查状态：Celery 先写 `STARTED` 再写 `SUCCESS`，中间状态会重新注册 oneshot 继续等待
4. 超时后清理 registry 防止内存泄漏

**与轮询方式对比：**

| 指标 | 轮询 (500ms) | 事件驱动 (Pub/Sub) |
|------|-------------|-------------------|
| Redis GET 次数/任务 | ~2-4 次 | ~3-4 次（仅防竞态检查） |
| 延迟 | ~250ms 平均 | ~1ms |
| CPU 占用 | 持续轮询 | 零（挂起等待） |
| 并发连接 | N 个命令连接 | 1 个 Pub/Sub + 复用命令连接 |

## 踩坑记录

| 问题 | 原因 | 解决 |
|------|------|------|
| Worker `KeyError: 'delivery_tag'` | `properties` 缺少 `delivery_tag` | 添加 `delivery_tag: <uuid>` |
| `lpush` 编译警告 | Rust 2024 edition 类型推断变化 | 显式标注 `lpush::<_, _, ()>` |
| Worker 未消费 | 消息格式与 kombu 不兼容 | 确保 body 为 base64 编码的 `[args, kwargs, embed]` 三元组 |

## 可复用 Crate 使用方式

```toml
# Cargo.toml
[dependencies]
celery-redis-producer = { path = "../rust-celery-producer" }
serde_json = "1.0"
```

```rust
use celery_redis_producer::{Producer, ResultListener};
use serde_json::json;

async fn example() -> anyhow::Result<()> {
    let broker_url = std::env::var("REDIS_BROKER_URL")
        .unwrap_or_else(|_| "redis://localhost:6379/6".to_string());
    let backend_url = std::env::var("REDIS_BACKEND_URL")
        .unwrap_or_else(|_| "redis://localhost:6379/7".to_string());

    let producer = Producer::new(&broker_url)?;
    let listener = ResultListener::new(&backend_url).await?;

    let args = json!(["/repo", ["file.c"]]);
    let task_id = producer.enqueue("scan.task", args).await?;

    let result = listener.wait(&task_id, 30).await?;
    println!("{:?}", result);
    Ok(())
}
```

## 后续演进

- [x] ~~轮询升级为 Redis Keyspace Notifications + Pub/Sub~~ (已完成)
- [x] ~~提取可复用 Rust Crate `celery-redis-producer`~~ (已完成)
- [ ] 接入真实 AST 扫描逻辑（Rust 端）和安全编译选项分析（Python 端）

## License

MIT
