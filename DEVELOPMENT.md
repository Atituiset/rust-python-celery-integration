# 本地开发与测试指南

## 环境准备

### 必需工具

| 工具 | 用途 | 安装方式 |
|------|------|----------|
| Rust | Rust 编译器和 Cargo | `rustup default stable` |
| Python 3.12+ | Python 运行时 | 系统自带或 uv 管理 |
| uv | Python 包管理和虚拟环境 | `pip install uv` |
| Redis 7+ | 消息队列和结果存储 | `apt install redis-server` 或 Docker |
| Docker | 容器化部署（可选） | 官网下载 |

### 快速验证环境

```bash
# Rust
cargo --version       # >= 1.80

# Python
python3 --version     # >= 3.12
uv --version          # >= 0.4

# Redis
redis-server --version
redis-cli PING        # 应返回 PONG
```

---

## 项目启动（三终端模式）

### 终端 1：启动 Redis

```bash
# 方式一：系统 Redis（推荐开发使用）
redis-server --daemonize yes --port 6379
redis-cli CONFIG SET notify-keyspace-events KEA

# 方式二：Docker
docker compose up -d redis
```

> **注意**：`notify-keyspace-events KEA` 必须开启，否则 Rust 端的 Pub/Sub 事件监听无法工作。

### 终端 2：启动 Python Celery Worker

```bash
cd python-worker

# 首次：创建虚拟环境并安装依赖
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

### 终端 3：运行 Rust Producer

```bash
cd rust-producer
cargo run
```

预期输出：
```
[Producer] Task pushed: id=...
[Listener] Waiting for result...

========== RESULT ==========
Status : SUCCESS
Task ID: ...
Result : {
  "findings_count": 3,
  "repo_path": "/path/to/repo",
  "scanned_files": ["file1.c", "file2.c", "main.c"],
  "status": "ok"
}
============================
```

---

## 配置说明

Redis 连接地址通过项目根目录 `.env` 文件统一管理：

```bash
# .env
REDIS_BROKER_URL=redis://localhost:6379/6    # Celery broker（任务队列）
REDIS_BACKEND_URL=redis://localhost:6379/7   # Celery backend（结果存储）
```

- **Rust 端**：通过 `dotenvy::dotenv().ok()` 加载，未设置时回退到默认值
- **Python 端**：通过 `python-dotenv` 加载，未设置时回退到默认值

如需连接远端 Redis，直接修改 `.env` 中的 URL 即可（需确保远端已开启 Keyspace Notifications）。

---

## 测试

### Rust 测试

```bash
cd rust-celery-producer

# 运行所有测试（单元测试 + 集成测试 + doctests）
cargo test

# 仅运行单元测试
cargo test --lib

# 仅运行集成测试（需要 Redis 运行）
cargo test --test integration_test
```

#### 测试覆盖

| 测试文件 | 类型 | 说明 |
|----------|------|------|
| `src/protocol.rs` | 单元测试 | 验证 Celery v2 消息 JSON 序列化格式 |
| `src/producer.rs` | 单元测试 | 验证 Producer URL 解析（合法/非法/带认证） |
| `src/result.rs` | 单元测试 | 验证 DB URL 解析、CeleryResult 状态判断、反序列化 |
| `tests/integration_test.rs` | 集成测试 | 端到端测试：LPUSH → Pub/Sub → 结果获取 |

#### 集成测试前置条件

```bash
redis-server --daemonize yes --port 6379
redis-cli CONFIG SET notify-keyspace-events KEA
redis-cli -n 15 FLUSHDB   # 清理测试数据库
```

### Python 测试

```bash
cd python-worker

# 安装测试依赖
uv pip install pytest

# 运行测试
.venv/bin/pytest test_tasks.py -v
```

#### 测试覆盖

| 测试用例 | 说明 |
|----------|------|
| `test_scan_task_basic` | 基本文件列表扫描 |
| `test_scan_task_empty_files` | 空文件列表 |
| `test_scan_task_single_file` | 单文件扫描 |
| `test_scan_task_result_structure` | 结果字段完整性验证 |
| `test_scan_task_task_id_preserved` | 任务上下文正确传递 |

> Python 测试使用 `task_always_eager = True` 模式，无需启动 Worker 即可同步执行 Celery 任务。

### 全量端到端集成测试

覆盖完整链路：**Rust Producer → Redis → Python Celery Worker → Redis → Rust Listener**

```bash
bash tests/end_to_end.sh
```

该脚本会自动完成以下步骤：

1. 检查/启动 Redis，开启 Keyspace Notifications
2. 清理测试数据库（DB 6 / DB 7）
3. 启动 Python Celery Worker（后台）
4. 编译并运行 Rust Producer
5. 验证输出包含所有预期字段

#### 验证点

| 检查项 | 说明 |
|--------|------|
| 任务推送成功 | `cargo run` 输出包含 `Task pushed` |
| 监听器启动 | 输出包含 `Waiting for result...` |
| 收到 SUCCESS 结果 | Worker 执行成功，Redis 推送事件 |
| 结果字段完整性 | 包含 `task_id`、`repo_path`、`scanned_files`、`findings_count` |

预期输出：
```
========================================
  测试报告
========================================
通过: 7
失败: 0

[INFO] 全量端到端测试全部通过！
```

> **前置条件**：系统已安装 `redis-server`、`cargo`、`uv`。脚本会自动创建 Python 虚拟环境和安装依赖。

---

## 参考运行日志

以下是在 **WSL2 / Ubuntu 22.04 / Linux 6.6.87** 环境下的实际运行输出，供其他环境部署时对比验证。

### Rust 单元测试 — `cargo test --lib`

```
running 10 tests
test result::tests::test_celery_result_deserialization ... ok
test result::tests::test_celery_result_is_final ... ok
test result::tests::test_parse_db_from_url_no_db ... ok
test result::tests::test_parse_db_from_url_with_auth ... ok
test protocol::tests::test_celery_message_serialization ... ok
test result::tests::test_parse_db_from_url_with_db ... ok
test protocol::tests::test_headers_defaults ... ok
test producer::tests::test_producer_new_invalid_url ... ok
test producer::tests::test_producer_new_valid_url ... ok
test producer::tests::test_producer_new_with_auth ... ok

test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

### Rust 集成测试 — `cargo test --test integration_test`

**前置条件**：Redis 已运行且开启 `notify-keyspace-events KEA`

```
running 3 tests
test test_parse_db_from_url ... ok
test test_enqueue_and_listen ... ok
test test_wait_timeout_returns_none ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

### Python 单元测试 — `pytest test_tasks.py -v`

```
============================== test session starts ==============================
platform linux -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
cachedir: .pytest_cache
rootdir: python-worker

test_tasks.py::TestScanTask::test_scan_task_basic PASSED                 [ 20%]
test_tasks.py::TestScanTask::test_scan_task_empty_files PASSED           [ 40%]
test_tasks.py::TestScanTask::test_scan_task_single_file PASSED           [ 60%]
test_tasks.py::TestScanTask::test_scan_task_result_structure PASSED      [ 80%]
test_tasks.py::TestScanTask::test_scan_task_task_id_preserved PASSED     [100%]

============================== 5 passed in 5.14s ===============================
```

### 全量端到端测试 — `bash tests/end_to_end.sh`

```
========================================
  Step 1: 启动 Redis
========================================
[INFO] 使用已运行的 Redis 实例
[INFO] Keyspace Notifications 已开启
[INFO] 测试数据库 (DB 6/7) 已清空

========================================
  Step 2: 启动 Python Celery Worker
========================================
[INFO] 启动 Worker...
[INFO] Worker 已启动 (PID: ...)

========================================
  Step 3: 构建 Rust Producer
========================================
[INFO] Rust Producer 构建完成

========================================
  Step 4: 运行全量端到端测试
========================================
[INFO] Rust Producer 推送任务并等待结果...
[Producer] Task pushed: id=7398c237-d237-4ed1-9e24-a93a36d18d34
[Listener] Waiting for result...

========== RESULT ==========
Status : SUCCESS
Task ID: 7398c237-d237-4ed1-9e24-a93a36d18d34
Result : {
  "findings_count": 3,
  "repo_path": "/path/to/repo",
  "scanned_files": [
    "file1.c",
    "file2.c",
    "main.c"
  ],
  "status": "ok"
}
============================

========================================
  Step 5: 验证测试结果
========================================
[INFO] 任务推送成功
[INFO] 监听器启动
[INFO] 收到 SUCCESS 结果
[INFO] 结果包含 task_id
[INFO] 结果包含 repo_path
[INFO] 结果包含 scanned_files
[INFO] 结果包含 findings_count

========================================
  测试报告
========================================
通过: 7
失败: 0

[INFO] 全量端到端测试全部通过！

[cleanup] 清理进程...
```

> **对比验证要点**：
> - Rust 测试输出应为 `10 passed` + `3 passed`
> - Python 测试输出应为 `5 passed`
> - E2E 测试最终退出码应为 `0`，报告 `通过: 7, 失败: 0`
> - 若结果不一致，优先检查 Redis Keyspace Notifications 是否开启

---

## 常见问题

### 1. Rust Producer 超时未收到结果

```
[Listener] No result received within timeout
```

**原因**：Redis Keyspace Notifications 未开启。

**解决**：
```bash
redis-cli CONFIG SET notify-keyspace-events KEA
```

### 2. Python Worker 未消费任务

**原因**：Worker 和 Producer 连接的不是同一个 Redis 实例或数据库。

**解决**：检查 `.env` 中的 `REDIS_BROKER_URL`，确保 Worker 和 Producer 使用相同的 broker 地址。

### 3. Cargo 编译失败

```bash
# 清理并重新构建
cargo clean && cargo build
```

### 4. Windows 上 Celery Worker 报 `PermissionError: [WinError 5]`

**原因**：Celery 默认使用 `prefork` 进程池（基于 `billiard`），依赖 Unix `fork()` 系统调用，Windows 不支持。

**解决**：`celeryconfig.py` 已自动检测 Windows 并降级为 `threads` 线程池，无需手动修改。如果仍有问题，启动时显式指定：

```bash
# Windows 原生
.venv\Scripts\celery -A tasks worker --loglevel=info -P threads

# 或单进程模式（最稳定，无并发）
.venv\Scripts\celery -A tasks worker --loglevel=info -P solo
```

### 5. Python 依赖冲突

```bash
cd python-worker
rm -rf .venv
uv venv
uv pip install -r requirements.txt
```

---

## 调试技巧

### 查看 Redis 中的任务状态

```bash
# 查看任务队列
redis-cli -n 6 LLEN celery
redis-cli -n 6 LRANGE celery 0 -1

# 查看任务结果
redis-cli -n 7 KEYS 'celery-task-meta-*'
redis-cli -n 7 GET celery-task-meta-<task_id>

# 监控 Keyspace 事件（调试用）
redis-cli --csv psubscribe '__keyevent@*__:set'
```

### 查看 Worker 日志

```bash
cd python-worker
.venv/bin/celery -A tasks worker --loglevel=debug
```

### Rust 调试输出

```bash
cd rust-producer
RUST_LOG=debug cargo run
```

---

## 项目结构速查

```
├── .env                              # Redis 连接配置
├── docker-compose.yml                # Redis 服务编排
├── python-worker/
│   ├── requirements.txt              # Python 依赖
│   ├── celeryconfig.py               # 从 .env 加载 Redis 配置
│   ├── tasks.py                      # Celery 任务定义
│   └── test_tasks.py                 # Python 单元测试
├── rust-celery-producer/             # 可复用 Rust Crate
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                    # 公开 API + doctest
│       ├── protocol.rs               # Celery v2 消息格式 + 测试
│       ├── producer.rs               # Producer + 测试
│       └── result.rs                 # ResultListener + 测试
│   └── tests/
│       ├── integration_test.rs       # Rust 单边集成测试
│       └── end_to_end.sh             # 全量跨语言端到端测试
└── rust-producer/                    # 示例应用
    ├── Cargo.toml
    └── src/main.rs                   # 读取 .env 并运行 Producer
```
