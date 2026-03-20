# spark-history-cli

A CLI for querying the Apache Spark History Server REST API.

## Prerequisites

- **Python 3.10+**
- **A running Spark History Server** (default: `http://localhost:18080`)

Start the History Server:
```bash
$SPARK_HOME/sbin/start-history-server.sh
```

## Installation

```bash
cd spark-history-cli
pip install -e .
```

Or install from PyPI:

```bash
pip install spark-history-cli
```

### Install as a Copilot CLI skill

```bash
spark-history-cli install-skill
```

This installs the bundled skill to `~/.copilot/skills/spark-history-cli`.

If Copilot CLI is already running, reload skills with:

```text
/skills reload
```

Then verify with `/skills list` and invoke it with prompts like:

```text
Use /spark-history-cli to inspect the latest completed SHS application.
```

## Usage

### REPL Mode (default)

```bash
spark-history-cli
# or specify a server:
spark-history-cli --server http://my-shs:18080
```

### One-Shot Commands

```bash
# List applications
spark-history-cli apps
spark-history-cli apps --status completed --limit 10

# Application details
spark-history-cli app <app-id>

# Jobs, stages, executors (requires --app-id or 'use' in REPL)
spark-history-cli --app-id <id> jobs
spark-history-cli --app-id <id> stages
spark-history-cli --app-id <id> executors --all
spark-history-cli --app-id <id> sql
spark-history-cli --app-id <id> env
spark-history-cli --app-id <id> summary

# SQL execution plans
spark-history-cli --app-id <id> sql-plan <exec-id>                # full plan
spark-history-cli --app-id <id> sql-plan <exec-id> --view initial # pre-AQE plan
spark-history-cli --app-id <id> sql-plan <exec-id> --view final   # post-AQE plan
spark-history-cli --app-id <id> sql-plan <exec-id> --dot          # Graphviz DOT
spark-history-cli --app-id <id> sql-plan <exec-id> --dot -o plan.dot  # save to file

# Jobs for a SQL execution
spark-history-cli --app-id <id> sql-jobs <exec-id>

# Download event logs
spark-history-cli --app-id <id> logs output.zip

# JSON output for scripting/agents
spark-history-cli --json apps
spark-history-cli --json --app-id <id> jobs
spark-history-cli --json --app-id <id> sql-plan <exec-id>
spark-history-cli --json --app-id <id> sql-jobs <exec-id>
```

### REPL Commands

```
apps                    List applications
app <id>                Show app details and set as current
use <id>                Set current app context
jobs                    List jobs for current app
job <id>                Show job details
job-stages <id>         Show stages for a job
stages                  List stages
stage <id> [attempt]    Show stage details
stage-summary <id>      Task metric quantiles (p5-p95)
stage-tasks <id>        List tasks (--length N, --sort-by)
executors [--all]       List executors
sql [id]                List or show SQL executions
sql-plan <id> [opts]    Show SQL plan (--view, --dot, -o)
sql-jobs <id>           Show jobs for a SQL execution
summary                 Application overview (config + workload)
rdds                    List cached RDDs
env                     Show environment/config
logs [path]             Download event logs
version                 Show Spark version
server <url>            Change server URL
status                  Show session state
help                    Show help
quit                    Exit
```

### Environment Variables

- `SPARK_HISTORY_SERVER` — Default server URL (overrides `http://localhost:18080`)

## API Coverage

Wraps all 20 endpoints of the Spark History Server REST API (`/api/v1/`):

- Applications (list, get, attempts)
- Jobs (list, get)
- Stages (list, get, attempts, task summary, task list)
- Executors (active, all)
- SQL Executions (list, get with plan graph)
- Storage (RDD list, detail)
- Environment
- Event Logs (download as ZIP)
- Miscellaneous Processes
- Version

## License

Apache License 2.0
