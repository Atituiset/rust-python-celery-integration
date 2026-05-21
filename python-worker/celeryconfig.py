import os
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
