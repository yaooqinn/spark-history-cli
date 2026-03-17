---
name: "spark-history-cli"
description: "CLI for querying the Apache Spark History Server REST API. List apps, jobs, stages, executors, SQL executions, and download event logs."
---

# spark-history-cli

CLI for querying the Apache Spark History Server REST API.

## Prerequisites

- Python 3.10+
- A running Spark History Server (default: http://localhost:18080)

## Installation

```bash
pip install spark-history-cli
```

## Basic Usage

```bash
# REPL mode (interactive)
spark-history-cli --server http://localhost:18080

# One-shot commands
spark-history-cli apps
spark-history-cli --json apps --limit 10
spark-history-cli app <app-id>
spark-history-cli --app-id <id> jobs
spark-history-cli --app-id <id> stages
spark-history-cli --app-id <id> executors --all
spark-history-cli --app-id <id> sql
spark-history-cli --app-id <id> env
spark-history-cli --app-id <id> logs output.zip
```

## Command Groups

| Command | Description |
|---------|-------------|
| `apps` | List applications (--status, --limit, --min-date, --max-date) |
| `app <id>` | Show application details |
| `jobs` | List jobs for current app (--status) |
| `job <id>` | Show job details |
| `stages` | List stages (--status) |
| `stage <id>` | Show stage details (--attempt) |
| `executors` | List executors (--all for dead executors) |
| `sql [id]` | List or show SQL executions (--offset, --length) |
| `rdds` | List cached RDDs |
| `env` | Show app environment and Spark config |
| `logs [path]` | Download event logs as ZIP |
| `version` | Show Spark version |

## Agent-Specific Guidance

- Use `--json` flag on all commands for machine-readable JSON output
- Use `--app-id` to set application context for subcommands
- Use `--server` or `SPARK_HISTORY_SERVER` env var for server URL
- REPL mode supports `use <app-id>` to set persistent app context
- All timestamps are in UTC, durations in human-readable format
- Error responses include HTTP status codes and descriptive messages
