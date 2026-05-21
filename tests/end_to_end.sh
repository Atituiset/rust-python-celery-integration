#!/usr/bin/env bash
# 全量端到端集成测试
# 覆盖完整链路: Rust Producer → Redis → Python Celery Worker → Redis → Rust Listener
#
# 用法:
#   cd /home/atituiset/Projects/rust-python-celery
#   bash tests/end_to_end.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REDIS_URL="${REDIS_BROKER_URL:-redis://127.0.0.1:6379}"
REPORT_FILE="/tmp/e2e_report_$$.txt"
WORKER_PID=""
REDIS_PID=""

# 颜色
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

cleanup() {
    echo ""
    echo "[cleanup] 清理进程..."
    # Kill Celery worker (also kills its fork pool children)
    if [[ -n "${WORKER_PID:-}" ]]; then
        pkill -P "$WORKER_PID" 2>/dev/null || true
        kill "$WORKER_PID" 2>/dev/null || true
    fi
    # Kill Redis only if we started it
    if [[ -n "${REDIS_PID:-}" ]] && kill -0 "$REDIS_PID" 2>/dev/null; then
        kill "$REDIS_PID" 2>/dev/null || true
    fi
    rm -f "$REPORT_FILE"
}
trap 'code=$?; cleanup; exit $code' EXIT

log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

step() {
    echo ""
    echo "========================================"
    echo "  $1"
    echo "========================================"
}

# ===== Step 1: 检查/启动 Redis =====
step "Step 1: 启动 Redis"

if redis-cli PING >/dev/null 2>&1; then
    log_info "使用已运行的 Redis 实例"
else
    log_info "启动 Redis..."
    redis-server --daemonize yes --port 6379
    REDIS_PID=$(pgrep -f "redis-server.*:6379" | head -1)
    sleep 1
    if ! redis-cli PING >/dev/null 2>&1; then
        log_error "Redis 启动失败"
        exit 1
    fi
    log_info "Redis 已启动 (PID: $REDIS_PID)"
fi

# 开启 Keyspace Notifications
redis-cli CONFIG SET notify-keyspace-events KEA >/dev/null
log_info "Keyspace Notifications 已开启"

# 清理测试数据库
redis-cli -n 6 FLUSHDB >/dev/null
redis-cli -n 7 FLUSHDB >/dev/null
log_info "测试数据库 (DB 6/7) 已清空"

# ===== Step 2: 启动 Python Worker =====
step "Step 2: 启动 Python Celery Worker"

cd "$PROJECT_ROOT/python-worker"

if [[ ! -d ".venv" ]]; then
    log_info "创建 Python 虚拟环境..."
    uv venv
fi

if ! .venv/bin/python -c "import celery" 2>/dev/null; then
    log_info "安装 Python 依赖..."
    uv pip install -r requirements.txt >/dev/null 2>&1
fi

log_info "启动 Worker..."
nohup .venv/bin/celery -A tasks worker --loglevel=warning > /tmp/celery_e2e.log 2>&1 &
WORKER_PID=$!
disown "$WORKER_PID" 2>/dev/null || true
sleep 3

if ! kill -0 "$WORKER_PID" 2>/dev/null; then
    log_error "Worker 启动失败"
    cat /tmp/celery_e2e.log
    exit 1
fi
log_info "Worker 已启动 (PID: $WORKER_PID)"

# ===== Step 3: 构建 Rust Producer =====
step "Step 3: 构建 Rust Producer"

cd "$PROJECT_ROOT/rust-producer"
if ! cargo build --quiet 2>/dev/null; then
    log_info "编译 Rust Producer..."
    cargo build
fi
log_info "Rust Producer 构建完成"

# ===== Step 4: 运行端到端测试 =====
step "Step 4: 运行全量端到端测试"

log_info "Rust Producer 推送任务并等待结果..."
cd "$PROJECT_ROOT/rust-producer"
timeout 30 cargo run --quiet 2>&1 | tee "$REPORT_FILE"

# ===== Step 5: 验证结果 =====
step "Step 5: 验证测试结果"

PASS=0
FAIL=0

check() {
    local desc="$1"
    local pattern="$2"
    if grep -q "$pattern" "$REPORT_FILE"; then
        log_info "✓ $desc"
        PASS=$((PASS + 1))
    else
        log_error "✗ $desc"
        FAIL=$((FAIL + 1))
    fi
}

check "任务推送成功" "Task pushed"
check "监听器启动" "Waiting for result"
check "收到 SUCCESS 结果" "Status : SUCCESS"
check "结果包含 task_id" "Task ID:"
check "结果包含 repo_path" '"repo_path"'
check "结果包含 scanned_files" '"scanned_files"'
check "结果包含 findings_count" '"findings_count"'

echo ""
echo "========================================"
echo "  测试报告"
echo "========================================"
echo -e "通过: ${GREEN}$PASS${NC}"
echo -e "失败: ${RED}$FAIL${NC}"

if [[ $FAIL -gt 0 ]]; then
    echo ""
    echo "--- Worker 日志 ---"
    cat /tmp/celery_e2e.log
    exit 1
fi

echo ""
log_info "全量端到端测试全部通过！"
exit 0
