# Changelog

## [1.1.0] - 2025-03-19

### Added
- **`sql-plan` command** — Extract and display SQL execution plans from the History Server.
  - `--view full|initial|final` to select which plan section to display (default: `full`).
  - `--view initial` shows the pre-AQE logical/physical plan.
  - `--view final` shows the post-AQE optimized plan.
  - `--dot` outputs the plan DAG as a Graphviz DOT file for visualization.
  - `-o <file>` writes output to a file instead of stdout.
  - `--json` returns structured JSON with `isAdaptive`, `sectionCount`, and parsed `sections`.
- `sql-plan` REPL command with the same options.

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
