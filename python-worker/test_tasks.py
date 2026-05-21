"""Tests for python-worker Celery tasks.

Run with:
    cd python-worker
    uv pip install pytest
    .venv/bin/pytest test_tasks.py -v

Or:
    python -m pytest test_tasks.py -v
"""

import pytest
from tasks import scan_task, app


# Run tasks synchronously (no worker needed)
app.conf.task_always_eager = True
app.conf.task_store_eager_result = True


class TestScanTask:
    """Test cases for scan.task Celery worker task."""

    def test_scan_task_basic(self):
        """Test scan_task with a basic file list."""
        repo_path = "/path/to/repo"
        file_list = ["main.c", "utils.c", "header.h"]

        result = scan_task.apply(args=[repo_path, file_list]).get()

        assert result["status"] == "ok"
        assert result["repo_path"] == repo_path
        assert result["scanned_files"] == file_list
        assert result["findings_count"] == len(file_list)

    def test_scan_task_empty_files(self):
        """Test scan_task with empty file list."""
        repo_path = "/empty/repo"
        file_list = []

        result = scan_task.apply(args=[repo_path, file_list]).get()

        assert result["status"] == "ok"
        assert result["scanned_files"] == []
        assert result["findings_count"] == 0

    def test_scan_task_single_file(self):
        """Test scan_task with a single file."""
        repo_path = "/single"
        file_list = ["main.c"]

        result = scan_task.apply(args=[repo_path, file_list]).get()

        assert result["status"] == "ok"
        assert result["findings_count"] == 1
        assert result["scanned_files"] == ["main.c"]

    def test_scan_task_result_structure(self):
        """Test that result contains all expected keys."""
        result = scan_task.apply(args=["/repo", ["a.c", "b.c"]]).get()

        expected_keys = {"status", "repo_path", "scanned_files", "findings_count"}
        assert set(result.keys()) == expected_keys

    def test_scan_task_task_id_preserved(self):
        """Test that the task result is returned correctly."""
        result = scan_task.apply(args=["/repo", ["a.c"]]).get()
        assert result is not None
        assert result["status"] == "ok"
