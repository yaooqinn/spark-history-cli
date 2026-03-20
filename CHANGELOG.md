# Changelog

## [1.3.0] - 2025-03-20

### Added
- **`stage-summary` command** — Task metric quantiles (p5/p25/p50/p75/p95) for duration, GC, memory, shuffle, and I/O.
- **`stage-tasks` command** — Individual task listing with sort support (`--sort-by ID|runtime|-runtime`) and pagination.
- **`job-stages` command** — Show stages belonging to a job.
- **`attempts` command** — List all attempts for an application.
- **`attempt` command** — Show details for a specific attempt (duration, log source, etc.).
- **`processes` command** — List miscellaneous processes (streaming receivers, etc.).
- **`rdd` command** — Show details for a specific cached RDD.
- All new commands available in both one-shot CLI and REPL modes.
- Full SHS REST API coverage — all 20 endpoints now have CLI commands.

## [1.2.0] - 2025-03-19

### Added
- **`sql-jobs` command** — Show jobs associated with a SQL execution.
  - Fetches all job IDs (succeeded, failed, running) from the SQL execution.
  - Uses bulk `list_jobs` + client-side filter for efficiency.
  - Gracefully handles missing job IDs (e.g., Gluten/Velox native engine apps).
- **`summary` command** — Concise application overview in a single view.
  - Application info: name, status, duration, Spark version, master, user.
  - Resource config: driver/executor memory & cores, shuffle partitions, serializer.
  - Workload stats: jobs, stages, tasks, SQL executions with status breakdowns.
- `sql-jobs` and `summary` REPL commands.

## [1.1.0] - 2025-03-19

### Added
- **`sql-plan` command** — Extract and display SQL execution plans from the History Server.
  - `--view full|initial|final` to select which plan section to display (default: `full`).
  - `--view initial` shows the pre-AQE logical/physical plan.
  - `--view final` shows the post-AQE optimized plan.
  - `--dot` outputs the plan DAG as a Graphviz DOT file for visualization.
  - `-o <file>` writes output to a file instead of stdout.
  - `--json` returns structured JSON with `isAdaptive`, `sectionCount`, and parsed `sections`.
- **`sql-jobs` command** — Show jobs associated with a SQL execution.
  - Fetches all job IDs (succeeded, failed, running) from the SQL execution.
  - Displays job details in a table with status, stages, and task counts.
  - Gracefully handles cases where referenced job IDs are not found.
- `sql-plan` and `sql-jobs` REPL commands with the same options.

### Changed
- **E2E CI switched to Docker-based SHS** — Uses `apache/spark:4.0.0` Docker image with `actions/cache` for faster CI runs (~5s cached load vs ~2min download).

## [1.0.1] - 2025-03-18

### Added
- `install-skill` command to install the bundled Copilot CLI skill.
- Copilot CLI skill (`SKILL.md`) for AI-assisted SHS querying.
- GitHub Actions CI (unit tests on Python 3.10/3.12/3.13 + E2E tests).
- PyPI publish workflow triggered on GitHub releases.

## [1.0.0] - 2025-03-18

### Added
- Initial release.
- REPL and one-shot CLI modes.
- Full Spark History Server REST API coverage (20 endpoints).
- `--json` output for scripting and agent consumption.
- Commands: `apps`, `app`, `jobs`, `job`, `stages`, `stage`, `executors`, `sql`, `rdds`, `env`, `logs`, `version`.
