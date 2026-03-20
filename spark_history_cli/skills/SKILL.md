---
name: "spark-history-cli"
description: "Query a running Apache Spark History Server from Copilot CLI. Use this whenever the user wants to inspect SHS applications, jobs, stages, executors, SQL executions, environment details, or event logs, especially when they mention Spark History Server, SHS, event log history, benchmark runs, or application IDs."
compatibility: "Requires Python 3.10+, the spark-history-cli package, and network access to a running Spark History Server."
---

# spark-history-cli

Use this skill when the task is about exploring or debugging data exposed by a running Apache Spark History Server.

## Why use this skill

- It gives you a purpose-built CLI instead of scraping the Spark History Server web UI.
- It wraps the REST API cleanly and already handles attempt-ID resolution for multi-attempt apps.
- It supports `--json`, which makes downstream reasoning and comparisons much easier.

## Workflow

1. Prefer the CLI over raw REST calls.
2. Prefer `--json` unless the user explicitly wants a human-formatted table.
3. Use `--server <url>` or `SPARK_HISTORY_SERVER` to point at the right SHS. If the user does not specify one, assume `http://localhost:18080`.
4. Start broad, then drill down:
   - list applications
   - choose the relevant app
   - inspect jobs, stages, executors, SQL executions, environment, or logs
5. If the user says "latest app", "recent run", or similar, list apps first and choose the most relevant recent application before continuing.
6. If the CLI is unavailable, install it with `python -m pip install spark-history-cli` if tool permissions allow it.

## Command patterns

```bash
spark-history-cli --json --server http://localhost:18080 apps
spark-history-cli --json --server http://localhost:18080 app <app-id>
spark-history-cli --json --server http://localhost:18080 --app-id <app-id> jobs
spark-history-cli --json --server http://localhost:18080 --app-id <app-id> stages
spark-history-cli --json --server http://localhost:18080 --app-id <app-id> executors --all
spark-history-cli --json --server http://localhost:18080 --app-id <app-id> sql
spark-history-cli --json --server http://localhost:18080 --app-id <app-id> sql-plan <exec-id> --view final
spark-history-cli --server http://localhost:18080 --app-id <app-id> sql-plan <exec-id> --dot -o plan.dot
spark-history-cli --json --server http://localhost:18080 --app-id <app-id> sql-jobs <exec-id>
spark-history-cli --json --server http://localhost:18080 --app-id <app-id> summary
spark-history-cli --json --server http://localhost:18080 --app-id <app-id> env
spark-history-cli --server http://localhost:18080 --app-id <app-id> logs output.zip
```

If `spark-history-cli` is not on `PATH`, use:

```bash
python -m spark_history_cli --json apps
```

## What to reach for

- `apps` for recent runs, durations, status, and picking candidates
- `app <id>` for high-level details about one run
- `jobs`, `job <id>` for job-level failures or progress
- `stages`, `stage <id>` for task/stage bottlenecks
- `stage-summary <id>` for task metric quantiles (p5/p25/p50/p75/p95) — duration, GC, memory, shuffle, I/O
- `stage-tasks <id>` for individual task details — sorted by runtime to find stragglers
- `executors --all` for executor churn or skew investigations
- `sql` for SQL execution history and plan graph data
- `sql-plan <id>` for SQL plan extraction:
  - `--view full` (default): full plan text
  - `--view initial`: only the Initial Plan (pre-AQE)
  - `--view final`: only the Final Plan (post-AQE)
  - `--dot`: Graphviz DOT output for visualizing the plan DAG
  - `--json` + `--view`: structured JSON with `isAdaptive`, `sectionCount`, `plan`, and `sections`
  - `-o <file>`: write output to file instead of stdout
- `sql-jobs <id>` for jobs associated with a SQL execution (fetches all linked jobs by ID)
- `summary` for a concise application overview: app info, resource config (driver/executor/shuffle), and workload stats (jobs/stages/tasks/SQL)
- `env` for Spark config/runtime context
- `logs` only when the user explicitly wants the event log archive saved locally

## Practical guidance

- Preserve the user's server URL if they gave one explicitly.
- Summarize findings after retrieving JSON; do not dump raw JSON unless the user asked for it.
- Treat event logs and benchmark history as potentially sensitive. Download them only when necessary and keep them local.
- This CLI needs a running Spark History Server. It does not replace SHS and it does not parse raw event logs directly.
