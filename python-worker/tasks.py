import time
from celery import Celery

app = Celery("worker")
app.config_from_object("celeryconfig")


@app.task(
    name="scan.task",
    bind=True,
    time_limit=60,
    soft_time_limit=30,
)
def scan_task(self, repo_path: str, file_list: list):
    """Celery task invoked by Rust producer."""
    task_id = self.request.id
    print(f"[{task_id}] Scanning repo={repo_path}, files={len(file_list)}")

    # Simulate some work
    time.sleep(1)

    result = {
        "status": "ok",
        "repo_path": repo_path,
        "scanned_files": file_list,
        "findings_count": len(file_list),
    }
    print(f"[{task_id}] Done. Result={result}")
    return result


if __name__ == "__main__":
    app.start()
