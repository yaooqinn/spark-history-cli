# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""REST API client for the Spark History Server.

Wraps all /api/v1/* endpoints with typed methods.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urljoin

import requests


class HistoryServerError(Exception):
    """Raised when the History Server returns an error."""

    def __init__(self, status_code: int, message: str, url: str = ""):
        self.status_code = status_code
        self.url = url
        super().__init__(f"HTTP {status_code}: {message}" + (f" ({url})" if url else ""))


class SparkHistoryClient:
    """Client for the Spark History Server REST API (/api/v1)."""

    def __init__(
        self,
        server_url: str = "http://localhost:18080",
        timeout: int = 30,
        basic_auth_username: str | None = None,
        basic_auth_password: str | None = None,
    ):
        self.server_url = server_url.rstrip("/")
        self.base_url = f"{self.server_url}/api/v1"
        self.timeout = timeout
        self._session = requests.Session()
        if basic_auth_username is not None:
            self._session.auth = (basic_auth_username, basic_auth_password or "")
        self._attempt_cache: dict[str, str | None] = {}

    def _resolve_attempt(self, app_id: str) -> str:
        """Return the URL base for an app, auto-resolving the attempt ID.

        The SHS requires /applications/{appId}/{attemptId}/... for apps
        that have attempt IDs. This method fetches the app info once,
        caches the latest attempt ID, and returns the correct URL prefix.
        """
        if app_id not in self._attempt_cache:
            try:
                app = self._get(f"applications/{app_id}")
                attempts = app.get("attempts", [])
                attempt_id = attempts[0].get("attemptId") if attempts else None
                self._attempt_cache[app_id] = attempt_id
            except HistoryServerError:
                self._attempt_cache[app_id] = None
        attempt_id = self._attempt_cache[app_id]
        if attempt_id:
            return f"applications/{app_id}/{attempt_id}"
        return f"applications/{app_id}"

    def _get(self, path: str, params: dict | None = None, stream: bool = False) -> Any:
        """Make a GET request and return the JSON response."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        try:
            resp = self._session.get(url, params=params, timeout=self.timeout, stream=stream)
        except requests.ConnectionError:
            raise HistoryServerError(
                0, f"Cannot connect to Spark History Server at {self.server_url}. "
                "Is it running?", url
            )
        except requests.Timeout:
            raise HistoryServerError(0, f"Request timed out after {self.timeout}s", url)

        if stream:
            resp.raise_for_status()
            return resp

        if resp.status_code != 200:
            try:
                msg = resp.json().get("message", resp.text)
            except Exception:
                msg = resp.text
            raise HistoryServerError(resp.status_code, msg, url)

        return resp.json()

    # ── Version ───────────────────────────────────────────────────────

    def get_version(self) -> dict:
        return self._get("version")

    # ── Applications ──────────────────────────────────────────────────

    def list_applications(
        self,
        status: str | None = None,
        min_date: str | None = None,
        max_date: str | None = None,
        min_end_date: str | None = None,
        max_end_date: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        params: dict[str, Any] = {}
        if status:
            params["status"] = status
        if min_date:
            params["minDate"] = min_date
        if max_date:
            params["maxDate"] = max_date
        if min_end_date:
            params["minEndDate"] = min_end_date
        if max_end_date:
            params["maxEndDate"] = max_end_date
        if limit is not None:
            params["limit"] = limit
        return self._get("applications", params=params)

    def get_application(self, app_id: str) -> dict:
        return self._get(f"applications/{app_id}")

    def get_attempt(self, app_id: str, attempt_id: str) -> dict:
        return self._get(f"applications/{app_id}/{attempt_id}")

    # ── Jobs ──────────────────────────────────────────────────────────

    def list_jobs(self, app_id: str, status: str | None = None) -> list[dict]:
        params = {"status": status} if status else {}
        base = self._resolve_attempt(app_id)
        return self._get(f"{base}/jobs", params=params)

    def get_job(self, app_id: str, job_id: int) -> dict:
        base = self._resolve_attempt(app_id)
        return self._get(f"{base}/jobs/{job_id}")

    # ── Stages ────────────────────────────────────────────────────────

    def list_stages(
        self,
        app_id: str,
        status: str | None = None,
        details: bool = False,
    ) -> list[dict]:
        params: dict[str, Any] = {}
        if status:
            params["status"] = status
        if details:
            params["details"] = "true"
        base = self._resolve_attempt(app_id)
        return self._get(f"{base}/stages", params=params)

    def get_stage(self, app_id: str, stage_id: int) -> list[dict]:
        base = self._resolve_attempt(app_id)
        return self._get(f"{base}/stages/{stage_id}")

    def get_stage_attempt(
        self,
        app_id: str,
        stage_id: int,
        attempt_id: int,
        details: bool = True,
    ) -> dict:
        params = {"details": str(details).lower()}
        base = self._resolve_attempt(app_id)
        return self._get(
            f"{base}/stages/{stage_id}/{attempt_id}", params=params
        )

    def get_task_summary(
        self,
        app_id: str,
        stage_id: int,
        attempt_id: int,
        quantiles: str = "0.05,0.25,0.5,0.75,0.95",
    ) -> dict:
        base = self._resolve_attempt(app_id)
        return self._get(
            f"{base}/stages/{stage_id}/{attempt_id}/taskSummary",
            params={"quantiles": quantiles},
        )

    def list_tasks(
        self,
        app_id: str,
        stage_id: int,
        attempt_id: int,
        offset: int = 0,
        length: int = 20,
        sort_by: str = "ID",
    ) -> list[dict]:
        base = self._resolve_attempt(app_id)
        return self._get(
            f"{base}/stages/{stage_id}/{attempt_id}/taskList",
            params={"offset": offset, "length": length, "sortBy": sort_by},
        )

    # ── Executors ─────────────────────────────────────────────────────

    def list_executors(self, app_id: str) -> list[dict]:
        base = self._resolve_attempt(app_id)
        return self._get(f"{base}/executors")

    def list_all_executors(self, app_id: str) -> list[dict]:
        base = self._resolve_attempt(app_id)
        return self._get(f"{base}/allexecutors")

    # ── Storage ───────────────────────────────────────────────────────

    def list_rdds(self, app_id: str) -> list[dict]:
        base = self._resolve_attempt(app_id)
        return self._get(f"{base}/storage/rdd")

    def get_rdd(self, app_id: str, rdd_id: int) -> dict:
        base = self._resolve_attempt(app_id)
        return self._get(f"{base}/storage/rdd/{rdd_id}")

    # ── Environment ───────────────────────────────────────────────────

    def get_environment(self, app_id: str) -> dict:
        base = self._resolve_attempt(app_id)
        return self._get(f"{base}/environment")

    # ── SQL ────────────────────────────────────────────────────────────

    def list_sql(
        self,
        app_id: str,
        details: bool = True,
        plan_description: bool = True,
        offset: int = 0,
        length: int = 20,
    ) -> list[dict]:
        base = self._resolve_attempt(app_id)
        return self._get(
            f"{base}/sql",
            params={
                "details": str(details).lower(),
                "planDescription": str(plan_description).lower(),
                "offset": offset,
                "length": length,
            },
        )

    def get_sql(
        self,
        app_id: str,
        execution_id: int,
        details: bool = True,
        plan_description: bool = True,
    ) -> dict:
        base = self._resolve_attempt(app_id)
        return self._get(
            f"{base}/sql/{execution_id}",
            params={
                "details": str(details).lower(),
                "planDescription": str(plan_description).lower(),
            },
        )

    # ── Event Logs ────────────────────────────────────────────────────

    def download_logs(self, app_id: str, output_path: str) -> str:
        """Download event logs as a ZIP file. Returns the output path."""
        resp = self._get(f"applications/{app_id}/logs", stream=True)
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return output_path

    # ── Miscellaneous ─────────────────────────────────────────────────

    def list_misc_processes(self, app_id: str) -> list[dict]:
        base = self._resolve_attempt(app_id)
        return self._get(f"{base}/allmiscellaneousprocess")

    # ── Health check ──────────────────────────────────────────────────

    def check_health(self) -> bool:
        """Check if the History Server is reachable."""
        try:
            self.get_version()
            return True
        except Exception:
            return False
