"""Unit tests for spark-history-cli core modules.

All HTTP calls are mocked — no running SHS required.
"""

from __future__ import annotations

import json
import os
import sys
import subprocess
import tempfile
from unittest.mock import patch, MagicMock

import pytest

from spark_history_cli.core.client import SparkHistoryClient, HistoryServerError
from spark_history_cli.core.session import Session
from spark_history_cli.core import formatters as fmt


# ── Sample API Responses ──────────────────────────────────────────────

SAMPLE_VERSION = {"spark": "4.0.0"}

SAMPLE_APP = {
    "id": "app-20250101120000-0001",
    "name": "MySparkApp",
    "attempts": [{
        "attemptId": None,
        "startTime": "2025-01-01T12:00:00.000GMT",
        "endTime": "2025-01-01T12:30:00.000GMT",
        "lastUpdated": "2025-01-01T12:30:00.000GMT",
        "duration": 1800000,
        "sparkUser": "kent",
        "completed": True,
        "appSparkVersion": "4.0.0",
        "startTimeEpoch": 1735732800000,
        "endTimeEpoch": 1735734600000,
        "lastUpdatedEpoch": 1735734600000,
    }],
}

SAMPLE_JOB = {
    "jobId": 0,
    "name": "count at MyApp.scala:42",
    "description": "count",
    "submissionTime": "2025-01-01T12:00:05.000GMT",
    "completionTime": "2025-01-01T12:00:10.000GMT",
    "stageIds": [0, 1],
    "status": "SUCCEEDED",
    "numTasks": 200,
    "numActiveTasks": 0,
    "numCompletedTasks": 200,
    "numSkippedTasks": 0,
    "numFailedTasks": 0,
    "numKilledTasks": 0,
    "numCompletedIndices": 200,
    "numActiveStages": 0,
    "numCompletedStages": 2,
    "numSkippedStages": 0,
    "numFailedStages": 0,
    "killedTasksSummary": {},
}

SAMPLE_STAGE = {
    "status": "COMPLETE",
    "stageId": 0,
    "attemptId": 0,
    "numTasks": 100,
    "numActiveTasks": 0,
    "numCompleteTasks": 100,
    "numFailedTasks": 0,
    "numKilledTasks": 0,
    "numCompletedIndices": 100,
    "executorRunTime": 45000,
    "jvmGcTime": 1200,
    "inputBytes": 1048576,
    "outputBytes": 524288,
    "shuffleReadBytes": 2097152,
    "shuffleWriteBytes": 1048576,
    "memoryBytesSpilled": 0,
    "diskBytesSpilled": 0,
    "name": "count at MyApp.scala:42",
}

SAMPLE_EXECUTOR = {
    "id": "1",
    "hostPort": "worker1:45678",
    "isActive": True,
    "totalCores": 4,
    "maxTasks": 4,
    "activeTasks": 0,
    "failedTasks": 0,
    "completedTasks": 50,
    "totalTasks": 50,
    "memoryUsed": 104857600,
    "maxMemory": 1073741824,
    "diskUsed": 0,
}

SAMPLE_SQL = {
    "id": 0,
    "status": "COMPLETED",
    "description": "SELECT count(*) FROM table",
    "duration": 5000,
    "submissionTime": "2025-01-01T12:00:05.000GMT",
    "runningJobIds": [],
    "successJobIds": [0],
    "failedJobIds": [],
    "nodes": [],
    "edges": [],
}

SAMPLE_ENV = {
    "runtime": {
        "javaVersion": "17.0.1",
        "javaHome": "/usr/lib/jvm/java-17",
        "scalaVersion": "2.13.12",
    },
    "sparkProperties": [
        ["spark.app.name", "MySparkApp"],
        ["spark.master", "yarn"],
        ["spark.executor.memory", "4g"],
    ],
    "hadoopProperties": [],
    "systemProperties": [],
    "metricsProperties": [],
    "classpathEntries": [],
    "resourceProfiles": [],
}


# ── Client Tests (Mocked HTTP) ───────────────────────────────────────

class TestClient:

    def _mock_client(self, json_data, status_code=200):
        """Create a client with a mocked session and pre-filled attempt cache."""
        client = SparkHistoryClient("http://test:18080")
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.json.return_value = json_data
        mock_resp.text = json.dumps(json_data)
        mock_resp.raise_for_status = MagicMock()
        client._session.get = MagicMock(return_value=mock_resp)
        # Pre-fill attempt cache so sub-resource methods don't trigger
        # extra HTTP calls to resolve the attempt ID
        client._attempt_cache["app-1"] = None
        client._attempt_cache["app-20250101120000-0001"] = None
        return client

    def test_get_version(self):
        client = self._mock_client(SAMPLE_VERSION)
        result = client.get_version()
        assert result["spark"] == "4.0.0"

    def test_list_applications(self):
        client = self._mock_client([SAMPLE_APP])
        result = client.list_applications(status="completed", limit=10)
        assert len(result) == 1
        assert result[0]["id"] == "app-20250101120000-0001"
        # Verify params were passed
        call_args = client._session.get.call_args
        assert call_args[1]["params"]["status"] == "completed"
        assert call_args[1]["params"]["limit"] == 10

    def test_get_application(self):
        client = self._mock_client(SAMPLE_APP)
        result = client.get_application("app-20250101120000-0001")
        assert result["name"] == "MySparkApp"

    def test_list_jobs(self):
        client = self._mock_client([SAMPLE_JOB])
        result = client.list_jobs("app-1", status="succeeded")
        assert len(result) == 1
        assert result[0]["jobId"] == 0

    def test_get_job(self):
        client = self._mock_client(SAMPLE_JOB)
        result = client.get_job("app-1", 0)
        assert result["status"] == "SUCCEEDED"

    def test_list_stages(self):
        client = self._mock_client([SAMPLE_STAGE])
        result = client.list_stages("app-1")
        assert len(result) == 1
        assert result[0]["stageId"] == 0

    def test_list_executors(self):
        client = self._mock_client([SAMPLE_EXECUTOR])
        result = client.list_executors("app-1")
        assert len(result) == 1
        assert result[0]["id"] == "1"

    def test_list_all_executors(self):
        client = self._mock_client([SAMPLE_EXECUTOR])
        result = client.list_all_executors("app-1")
        assert len(result) == 1

    def test_list_sql(self):
        client = self._mock_client([SAMPLE_SQL])
        result = client.list_sql("app-1")
        assert len(result) == 1
        assert result[0]["status"] == "COMPLETED"

    def test_get_sql(self):
        client = self._mock_client(SAMPLE_SQL)
        result = client.get_sql("app-1", 0)
        assert result["id"] == 0

    def test_list_rdds(self):
        client = self._mock_client([])
        result = client.list_rdds("app-1")
        assert result == []

    def test_get_environment(self):
        client = self._mock_client(SAMPLE_ENV)
        result = client.get_environment("app-1")
        assert result["runtime"]["scalaVersion"] == "2.13.12"

    def test_connection_error(self):
        client = SparkHistoryClient("http://nonexistent:18080")
        import requests
        client._session.get = MagicMock(side_effect=requests.ConnectionError("refused"))
        with pytest.raises(HistoryServerError) as exc_info:
            client.get_version()
        assert "Cannot connect" in str(exc_info.value)

    def test_http_404(self):
        client = self._mock_client({"message": "not found"}, status_code=404)
        with pytest.raises(HistoryServerError) as exc_info:
            client.get_application("nonexistent")
        assert exc_info.value.status_code == 404

    def test_check_health_ok(self):
        client = self._mock_client(SAMPLE_VERSION)
        assert client.check_health() is True

    def test_check_health_fail(self):
        client = SparkHistoryClient("http://nonexistent:18080")
        import requests
        client._session.get = MagicMock(side_effect=requests.ConnectionError("refused"))
        assert client.check_health() is False


# ── Formatter Tests ───────────────────────────────────────────────────

class TestFormatters:

    def test_format_app_list(self):
        headers, rows = fmt.format_app_list([SAMPLE_APP])
        assert len(rows) == 1
        assert "App ID" in headers
        assert "app-20250101120000-0001" in rows[0][0]

    def test_format_app_detail(self):
        info = fmt.format_app_detail(SAMPLE_APP)
        assert info["Name"] == "MySparkApp"
        assert "COMPLETED" in info["Status"]

    def test_format_job_list(self):
        headers, rows = fmt.format_job_list([SAMPLE_JOB])
        assert len(rows) == 1
        assert "SUCCEEDED" in rows[0][2]

    def test_format_stage_list(self):
        headers, rows = fmt.format_stage_list([SAMPLE_STAGE])
        assert len(rows) == 1
        assert "100/100" in rows[0][4]  # tasks

    def test_format_executor_list(self):
        headers, rows = fmt.format_executor_list([SAMPLE_EXECUTOR])
        assert len(rows) == 1
        assert rows[0][0] == "1"

    def test_format_sql_list(self):
        headers, rows = fmt.format_sql_list([SAMPLE_SQL])
        assert len(rows) == 1
        assert "COMPLETED" in rows[0][1]

    def test_format_sql_detail(self):
        info = fmt.format_sql_detail(SAMPLE_SQL)
        assert info["Status"] == "COMPLETED"

    def test_format_environment(self):
        info = fmt.format_environment(SAMPLE_ENV)
        assert "17.0.1" in info["Java Version"]

    def test_format_spark_properties(self):
        headers, rows = fmt.format_spark_properties(SAMPLE_ENV)
        assert len(rows) == 3
        assert rows[0][0] == "spark.app.name"

    def test_duration_ms(self):
        assert fmt._duration(500) == "500ms"

    def test_duration_seconds(self):
        assert "s" in fmt._duration(5000)

    def test_duration_minutes(self):
        assert "m" in fmt._duration(120000)

    def test_duration_hours(self):
        assert "h" in fmt._duration(7200000)

    def test_duration_none(self):
        assert fmt._duration(None) == "N/A"

    def test_bytes_formatting(self):
        assert "B" in fmt._bytes(500)
        assert "KB" in fmt._bytes(2048)
        assert "MB" in fmt._bytes(1048576)
        assert "GB" in fmt._bytes(1073741824)
        assert fmt._bytes(None) == "N/A"


# ── Session Tests ─────────────────────────────────────────────────────

class TestSession:

    def test_set_app(self):
        session = Session()
        session.set_app("app-1", "1")
        assert session.current_app_id == "app-1"
        assert session.current_attempt_id == "1"

    def test_clear_app(self):
        session = Session()
        session.set_app("app-1")
        session.clear_app()
        assert session.current_app_id is None

    def test_require_app_raises(self):
        session = Session()
        with pytest.raises(ValueError, match="No application selected"):
            session.require_app()

    def test_require_app_returns(self):
        session = Session()
        session.set_app("app-1")
        assert session.require_app() == "app-1"

    def test_context_label_empty(self):
        session = Session()
        assert session.context_label == ""

    def test_context_label_short(self):
        session = Session()
        session.set_app("app-123")
        assert session.context_label == "app-123"

    def test_context_label_long(self):
        session = Session()
        session.set_app("application_1234567890123456_99999")
        label = session.context_label
        assert "..." in label
        assert len(label) < 35

    def test_context_label_with_attempt(self):
        session = Session()
        session.set_app("app-1", "2")
        assert session.context_label == "app-1/2"

    def test_save_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "session.json")
            session = Session(server_url="http://test:18080")
            session.set_app("app-1", "2")
            session.save(path)

            loaded = Session.load(path)
            assert loaded.server_url == "http://test:18080"
            assert loaded.current_app_id == "app-1"
            assert loaded.current_attempt_id == "2"

    def test_load_missing_file(self):
        session = Session.load("/nonexistent/session.json")
        assert session.server_url == "http://localhost:18080"
        assert session.current_app_id is None


# ── CLI Subprocess Tests ──────────────────────────────────────────────

def _resolve_cli(name):
    """Resolve installed CLI command; falls back to python -m for dev."""
    import shutil
    force = os.environ.get("CLI_ANYTHING_FORCE_INSTALLED", "").strip() == "1"
    path = shutil.which(name)
    if path:
        print(f"[_resolve_cli] Using installed command: {path}")
        return [path]
    if force:
        raise RuntimeError(f"{name} not found in PATH. Install with: pip install -e .")
    module = "spark_history_cli.cli"
    print(f"[_resolve_cli] Falling back to: {sys.executable} -m {module}")
    return [sys.executable, "-m", "spark_history_cli"]


class TestCLISubprocess:
    CLI_BASE = _resolve_cli("spark-history-cli")

    def _run(self, args, check=True):
        return subprocess.run(
            self.CLI_BASE + args,
            capture_output=True, text=True, check=check,
        )

    def test_help(self):
        result = self._run(["--help"])
        assert result.returncode == 0
        assert "Spark History Server" in result.stdout

    def test_version(self):
        from spark_history_cli import __version__
        result = self._run(["--version"])
        assert result.returncode == 0
        assert __version__ in result.stdout

    def test_apps_no_server(self):
        """When no server is running, should fail gracefully."""
        result = self._run(
            ["--server", "http://localhost:19999", "--json", "apps"],
            check=False,
        )
        # Should exit with error (non-zero) when no server
        assert result.returncode != 0 or "error" in result.stderr.lower() or "Cannot connect" in result.stderr

    def test_install_skill(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "spark-history-cli")
            result = self._run(["install-skill", "--target-dir", target])
            assert result.returncode == 0
            assert os.path.exists(os.path.join(target, "SKILL.md"))
            assert "Installed Copilot skill" in result.stdout
