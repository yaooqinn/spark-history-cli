"""E2E tests for spark-history-cli — requires a running Spark History Server.

Set SPARK_HISTORY_SERVER env var to the server URL (default: http://localhost:18080).
These tests are skipped if the server is not reachable.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import zipfile

import pytest

from spark_history_cli.core.client import SparkHistoryClient


SERVER_URL = os.environ.get("SPARK_HISTORY_SERVER", "http://localhost:18080")


def _server_available():
    """Check if the History Server is reachable."""
    try:
        client = SparkHistoryClient(SERVER_URL, timeout=5)
        return client.check_health()
    except Exception:
        return False


skip_no_server = pytest.mark.skipif(
    not _server_available(),
    reason=f"Spark History Server not available at {SERVER_URL}",
)


def _resolve_cli(name):
    """Resolve installed CLI command."""
    import shutil
    force = os.environ.get("CLI_ANYTHING_FORCE_INSTALLED", "").strip() == "1"
    path = shutil.which(name)
    if path:
        print(f"[_resolve_cli] Using installed command: {path}")
        return [path]
    if force:
        raise RuntimeError(f"{name} not found in PATH. Install with: pip install -e .")
    print(f"[_resolve_cli] Falling back to: {sys.executable} -m spark_history_cli")
    return [sys.executable, "-m", "spark_history_cli"]


@skip_no_server
class TestE2EWithServer:
    """E2E tests that require a running Spark History Server."""

    client = SparkHistoryClient(SERVER_URL)

    def test_version(self):
        ver = self.client.get_version()
        assert "spark" in ver
        print(f"\n  Spark version: {ver['spark']}")

    def test_list_applications(self):
        apps = self.client.list_applications()
        assert isinstance(apps, list)
        print(f"\n  Applications found: {len(apps)}")
        for app in apps[:3]:
            print(f"    - {app['id']}: {app['name']}")

    def test_get_first_application(self):
        apps = self.client.list_applications(limit=1)
        if not apps:
            pytest.skip("No applications in history server")
        app = self.client.get_application(apps[0]["id"])
        assert app["id"] == apps[0]["id"]
        assert "attempts" in app

    def test_list_jobs(self):
        apps = self.client.list_applications(limit=1)
        if not apps:
            pytest.skip("No applications")
        jobs = self.client.list_jobs(apps[0]["id"])
        assert isinstance(jobs, list)
        print(f"\n  Jobs for {apps[0]['id']}: {len(jobs)}")

    def test_list_stages(self):
        apps = self.client.list_applications(limit=1)
        if not apps:
            pytest.skip("No applications")
        stages = self.client.list_stages(apps[0]["id"])
        assert isinstance(stages, list)
        print(f"\n  Stages for {apps[0]['id']}: {len(stages)}")

    def test_list_executors(self):
        apps = self.client.list_applications(limit=1)
        if not apps:
            pytest.skip("No applications")
        execs = self.client.list_all_executors(apps[0]["id"])
        assert isinstance(execs, list)
        print(f"\n  Executors for {apps[0]['id']}: {len(execs)}")

    def test_list_sql(self):
        apps = self.client.list_applications(limit=1)
        if not apps:
            pytest.skip("No applications")
        sqls = self.client.list_sql(apps[0]["id"])
        assert isinstance(sqls, list)
        print(f"\n  SQL executions for {apps[0]['id']}: {len(sqls)}")

    def test_get_environment(self):
        apps = self.client.list_applications(limit=1)
        if not apps:
            pytest.skip("No applications")
        env = self.client.get_environment(apps[0]["id"])
        assert "runtime" in env
        assert "sparkProperties" in env
        print(f"\n  Java: {env['runtime'].get('javaVersion')}")
        print(f"  Scala: {env['runtime'].get('scalaVersion')}")

    def test_download_logs(self):
        apps = self.client.list_applications(limit=1)
        if not apps:
            pytest.skip("No applications")
        with tempfile.TemporaryDirectory() as tmpdir:
            out = os.path.join(tmpdir, "logs.zip")
            path = self.client.download_logs(apps[0]["id"], out)
            assert os.path.exists(path)
            size = os.path.getsize(path)
            assert size > 0
            # Verify it's a valid ZIP
            assert zipfile.is_zipfile(path)
            print(f"\n  Event logs: {path} ({size:,} bytes)")

    def test_full_workflow(self):
        """Full investigation workflow: apps → app → jobs → stages → execs → env."""
        apps = self.client.list_applications(limit=1)
        if not apps:
            pytest.skip("No applications")

        app_id = apps[0]["id"]
        print(f"\n  Investigating app: {app_id}")

        app = self.client.get_application(app_id)
        assert app["name"]
        print(f"  App name: {app['name']}")

        jobs = self.client.list_jobs(app_id)
        print(f"  Jobs: {len(jobs)}")

        stages = self.client.list_stages(app_id)
        print(f"  Stages: {len(stages)}")

        execs = self.client.list_all_executors(app_id)
        print(f"  Executors: {len(execs)}")

        env = self.client.get_environment(app_id)
        print(f"  Spark version: {env['runtime'].get('scalaVersion')}")


@skip_no_server
class TestCLISubprocessE2E:
    """E2E subprocess tests using the installed CLI."""

    CLI_BASE = _resolve_cli("spark-history-cli")

    def _run(self, args, check=True):
        return subprocess.run(
            self.CLI_BASE + ["--server", SERVER_URL] + args,
            capture_output=True, text=True, check=check,
        )

    def test_json_apps(self):
        result = self._run(["--json", "apps"])
        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert isinstance(data, list)

    def test_json_version(self):
        result = self._run(["--json", "version"])
        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert "spark" in data

    def test_full_workflow_subprocess(self):
        """Full workflow via subprocess: list apps → get details."""
        result = self._run(["--json", "apps", "--limit", "1"])
        assert result.returncode == 0
        apps = json.loads(result.stdout)
        if not apps:
            pytest.skip("No applications")

        app_id = apps[0]["id"]
        result = self._run(["--json", "app", app_id])
        assert result.returncode == 0
        app = json.loads(result.stdout)
        assert app["id"] == app_id

        result = self._run(["--json", "--app-id", app_id, "jobs"])
        assert result.returncode == 0
        jobs = json.loads(result.stdout)
        assert isinstance(jobs, list)
        print(f"\n  Subprocess workflow: app={app_id}, jobs={len(jobs)}")
