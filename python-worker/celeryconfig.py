import os
import platform
from dotenv import load_dotenv

# Load environment variables from project root .env
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# Celery configuration
broker_url = os.getenv("REDIS_BROKER_URL", "redis://localhost:6379/6")
result_backend = os.getenv("REDIS_BACKEND_URL", "redis://localhost:6379/7")

task_serializer = "json"
accept_content = ["json"]
result_serializer = "json"
timezone = "UTC"
enable_utc = True

# Acknowledge after task completes, not before
task_acks_late = True

# Store task results
task_track_started = True
result_expires = 3600

# Concurrency
worker_concurrency = 2

# Windows 兼容: prefork 进程池依赖 Unix fork(), Windows 不支持
# 自动降级为 threads 或 solo
if platform.system() == "Windows":
    # 使用线程池代替进程池, 避免 billiard PermissionError
    worker_pool = "threads"

    # 可选: 如果 threads 也有问题, 用 solo(单进程, 无并发)
    # worker_pool = "solo"

    # 消除 billiard 的 fork 警告
    os.environ.setdefault("FORKED_BY_MULTIPROCESSING", "1")
