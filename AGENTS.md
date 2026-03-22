# AGENTS.md

## Project Overview

**spark-history-cli** is a Python CLI for querying the Apache Spark History Server REST API. It also ships two agent skills (`spark-history-cli` and `spark-advisor`) that teach AI coding assistants how to debug and optimize Spark applications.

## Repository Structure

```
spark-history-cli/
├── spark_history_cli/       # Python package
│   ├── cli.py               # Click-based CLI entry point (REPL + one-shot)
│   ├── core/                # API client, formatters, data models
│   ├── utils/               # Skill installer, helpers
│   ├── tests/               # pytest test suite
│   └── skills/              # Bundled SKILL.md (installed via `install-skill` command)
├── skills/                  # Root-level skills (discovered by `npx skills add`)
│   ├── spark-history-cli/   # CLI usage skill
│   │   ├── SKILL.md
│   │   └── sample_codes/
│   └── spark-advisor/       # Performance diagnosis skill
│       ├── SKILL.md
│       ├── references/      # diagnostics.md, comparison.md
│       └── sample_codes/
├── setup.py                 # Package metadata, version, entry points
├── CHANGELOG.md             # Release history
└── .github/workflows/
    ├── ci.yml               # Tests on push/PR
    └── publish.yml          # PyPI publish on GitHub Release
```

## Key Development Guidelines

- **Python 3.10+** required
- Install locally: `pip install -e .`
- Run tests: `pytest spark_history_cli/tests/`
- Lint: standard Python conventions, no custom linter configured
- Entry point: `spark_history_cli.cli:main`

## Making Changes

### CLI commands
All commands are in `cli.py` using Click. Each command calls methods on the API client in `core/`. Add new commands by following the existing pattern (Click command → API call → format output).

### Skills
Skills are pure Markdown (SKILL.md) with optional `references/` and `sample_codes/`. The skills in `skills/` (root) are the canonical source — `spark_history_cli/skills/` is a bundled copy for the `install-skill` command.

When updating skills, edit in `skills/` first, then sync to `spark_history_cli/skills/`.

### Diagnostics
All diagnostic rules live in `skills/spark-advisor/references/diagnostics.md`. Rules use data from the standard Spark History Server REST API (`/api/v1/`). No external plugins required.

### Releasing
1. Bump version in `setup.py`
2. Update `CHANGELOG.md`
3. Commit and tag: `git tag v{X.Y.Z}`
4. Create a GitHub Release — `publish.yml` auto-publishes to PyPI via trusted publishing

## Testing

A running Spark History Server is needed for integration tests. Unit tests mock the API. Run:

```bash
pytest spark_history_cli/tests/ -v
```

## Commit Style

Use conventional-ish prefixes: `feat:`, `fix:`, `docs:`, `skills:`, `release:`, `chore:`.
