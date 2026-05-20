# Celery configuration
# DB 6: broker (task queue)
# DB 7: result backend

broker_url = "redis://localhost:6379/6"
result_backend = "redis://localhost:6379/7"

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
