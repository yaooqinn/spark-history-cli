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
