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
"""Backend module: validates connectivity to a running Spark History Server."""

from __future__ import annotations

from spark_history_cli.core.client import SparkHistoryClient, HistoryServerError


def check_server(server_url: str = "http://localhost:18080") -> dict:
    """Check that the Spark History Server is reachable and return version info.

    Raises RuntimeError with clear instructions if not reachable.
    """
    client = SparkHistoryClient(server_url)
    try:
        version = client.get_version()
        return {
            "server_url": server_url,
            "spark_version": version.get("spark", "unknown"),
            "status": "ok",
        }
    except HistoryServerError as e:
        raise RuntimeError(
            f"Cannot connect to Spark History Server at {server_url}.\n"
            f"Error: {e}\n\n"
            "Make sure the History Server is running:\n"
            "  $SPARK_HOME/sbin/start-history-server.sh\n\n"
            "Or specify a different URL:\n"
            "  spark-history-cli --server http://host:18080\n"
        ) from e
