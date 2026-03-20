"""spark-history-cli: CLI for querying the Apache Spark History Server.

Usage:
    spark-history-cli [OPTIONS] COMMAND [ARGS]...
    spark-history-cli                          # enters REPL mode
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click

from spark_history_cli import __version__
from spark_history_cli.core.client import SparkHistoryClient, HistoryServerError
from spark_history_cli.core.session import Session
from spark_history_cli.core import formatters as fmt
from spark_history_cli.utils.skill_install import default_skill_target, install_copilot_skill


# ── Shared state via Click context ────────────────────────────────────

class CliState:
    def __init__(self):
        self.client: SparkHistoryClient | None = None
        self.session: Session = Session()
        self.json_mode: bool = False

    def ensure_client(self) -> SparkHistoryClient:
        if self.client is None:
            self.client = SparkHistoryClient(self.session.server_url)
        return self.client

    def resolve_app_id(self, app_id: str | None) -> str:
        if app_id:
            return app_id
        return self.session.require_app()


pass_state = click.make_pass_decorator(CliState, ensure=True)


def output_json(data: Any):
    """Print data as formatted JSON."""
    click.echo(json.dumps(data, indent=2, default=str))


def output_table(skin, headers: list[str], rows: list[list[str]]):
    """Print data as a formatted table using the REPL skin."""
    if not rows:
        skin.info("No results.")
        return
    skin.table(headers, rows)


def output_status_block(skin, info: dict[str, str], title: str = ""):
    """Print a key-value status block."""
    skin.status_block(info, title=title)


def _collect_sql_job_ids(sql_exec: dict) -> list[int]:
    """Collect all job IDs from a SQL execution (success + failed + running)."""
    ids = []
    ids.extend(sql_exec.get("successJobIds", []))
    ids.extend(sql_exec.get("failedJobIds", []))
    ids.extend(sql_exec.get("runningJobIds", []))
    return sorted(set(ids))


def _fetch_sql_jobs(client, app_id: str, sql_exec: dict) -> list[dict]:
    """Fetch job details for a SQL execution using bulk list + filter."""
    job_ids = _collect_sql_job_ids(sql_exec)
    if not job_ids:
        return []
    target = set(job_ids)
    all_jobs = client.list_jobs(app_id)
    return [j for j in all_jobs if j.get("jobId") in target]


# ── Main CLI group ────────────────────────────────────────────────────

@click.group(invoke_without_command=True)
@click.option("--server", "-s", default="http://localhost:18080",
              envvar="SPARK_HISTORY_SERVER",
              help="Spark History Server URL (default: http://localhost:18080)")
@click.option("--json", "json_mode", is_flag=True, default=False,
              help="Output in JSON format for machine consumption")
@click.option("--app-id", "-a", default=None,
              help="Application ID to use (sets context for subcommands)")
@click.version_option(__version__, prog_name="spark-history-cli")
@click.pass_context
def cli(ctx, server: str, json_mode: bool, app_id: str | None):
    """CLI for querying the Apache Spark History Server REST API."""
    state = CliState()
    state.session.server_url = server
    state.json_mode = json_mode
    if app_id:
        state.session.set_app(app_id)
    state.ensure_client()
    ctx.obj = state

    if ctx.invoked_subcommand is None:
        ctx.invoke(repl)


# ── REPL command ──────────────────────────────────────────────────────

@cli.command(hidden=True)
@pass_state
def repl(state: CliState):
    """Interactive REPL mode."""
    from spark_history_cli.utils.repl_skin import ReplSkin

    skin = ReplSkin("spark_history", version=__version__)
    skin.print_banner()

    # Check connectivity
    client = state.ensure_client()
    try:
        ver = client.get_version()
        skin.success(f"Connected to {state.session.server_url} (Spark {ver.get('spark', '?')})")
    except HistoryServerError as e:
        skin.error(f"Cannot connect to {state.session.server_url}: {e}")
        skin.hint("Use 'server <url>' to change the server URL.")

    commands_help = {
        "apps": "List applications (apps [--status completed|running] [--limit N])",
        "app <id>": "Show application details and set as current",
        "attempts": "List attempts for current app",
        "attempt <id>": "Show attempt details",
        "use <id>": "Set current application context",
        "jobs": "List jobs for current app",
        "job <id>": "Show job details",
        "job-stages <id>": "Show stages for a job",
        "stages": "List stages for current app",
        "stage <id>": "Show stage details",
        "stage-summary <id>": "Task metric quantiles (p5/p25/p50/p75/p95)",
        "stage-tasks <id>": "List tasks (--length N, --sort-by FIELD)",
        "executors": "List executors for current app",
        "sql": "List SQL executions for current app",
        "sql <id>": "Show SQL execution details",
        "sql-plan <id>": "Show SQL plan (--view initial|final|full, --dot for Graphviz)",
        "sql-jobs <id>": "Show jobs for a SQL execution",
        "summary": "Application overview (config + workload)",
        "processes": "List miscellaneous processes",
        "rdds": "List cached RDDs for current app",
        "rdd <id>": "Show RDD details",
        "env": "Show application environment",
        "logs <path>": "Download event logs to file",
        "version": "Show Spark version",
        "server <url>": "Change History Server URL",
        "status": "Show current session state",
        "help": "Show this help",
        "quit": "Exit the REPL",
    }

    pt_session = skin.create_prompt_session()

    while True:
        try:
            line = skin.get_input(
                pt_session,
                project_name=state.session.context_label,
                context=state.session.context_label,
            )
        except (EOFError, KeyboardInterrupt):
            skin.print_goodbye()
            break

        if not line:
            continue

        parts = line.split()
        cmd = parts[0].lower()
        args = parts[1:]

        try:
            if cmd in ("quit", "exit", "q"):
                skin.print_goodbye()
                break

            elif cmd == "help":
                skin.help(commands_help)

            elif cmd == "version":
                ver = client.get_version()
                skin.status("Spark Version", ver.get("spark", "unknown"))

            elif cmd == "server":
                if args:
                    state.session.server_url = args[0]
                    state.client = SparkHistoryClient(args[0])
                    client = state.client
                    try:
                        ver = client.get_version()
                        skin.success(f"Connected to {args[0]} (Spark {ver.get('spark', '?')})")
                    except HistoryServerError as e:
                        skin.error(f"Cannot connect: {e}")
                else:
                    skin.status("Server", state.session.server_url)

            elif cmd == "status":
                skin.status_block({
                    "Server": state.session.server_url,
                    "Current App": state.session.current_app_id or "(none)",
                    "Attempt": state.session.current_attempt_id or "(none)",
                }, title="Session")

            elif cmd == "use":
                if not args:
                    skin.error("Usage: use <app-id> [attempt-id]")
                else:
                    attempt = args[1] if len(args) > 1 else None
                    state.session.set_app(args[0], attempt)
                    skin.success(f"Context set to {state.session.context_label}")

            elif cmd == "apps":
                params: dict[str, Any] = {}
                # Simple arg parsing: --status X --limit N
                i = 0
                while i < len(args):
                    if args[i] == "--status" and i + 1 < len(args):
                        params["status"] = args[i + 1]
                        i += 2
                    elif args[i] == "--limit" and i + 1 < len(args):
                        params["limit"] = int(args[i + 1])
                        i += 2
                    else:
                        i += 1
                apps = client.list_applications(**params)
                headers, rows = fmt.format_app_list(apps)
                output_table(skin, headers, rows)

            elif cmd == "app":
                if not args:
                    if state.session.current_app_id:
                        app = client.get_application(state.session.current_app_id)
                        info = fmt.format_app_detail(app)
                        output_status_block(skin, info, title="Application")
                    else:
                        skin.error("Usage: app <app-id>")
                else:
                    app_id = args[0]
                    app = client.get_application(app_id)
                    state.session.set_app(app_id)
                    info = fmt.format_app_detail(app)
                    output_status_block(skin, info, title="Application")
                    skin.hint(f"Context set to {app_id}")

            elif cmd == "attempts":
                app_id = state.resolve_app_id(None)
                app = client.get_application(app_id)
                attempts = app.get("attempts", [])
                headers, rows = fmt.format_attempt_list(attempts)
                output_table(skin, headers, rows)

            elif cmd == "attempt":
                if not args:
                    skin.error("Usage: attempt <attempt-id>")
                else:
                    app_id = state.session.require_app()
                    data = client.get_attempt(app_id, args[0])
                    info = fmt.format_attempt_detail(data)
                    output_status_block(skin, info, title=f"Attempt {args[0]}")

            elif cmd == "processes":
                app_id = state.resolve_app_id(None)
                procs = client.list_misc_processes(app_id)
                headers, rows = fmt.format_process_list(procs)
                output_table(skin, headers, rows)

            elif cmd == "summary":
                app_id = state.resolve_app_id(None)
                app = client.get_application(app_id)
                env = client.get_environment(app_id)
                jobs = client.list_jobs(app_id)
                stages = client.list_stages(app_id)
                executors = client.list_all_executors(app_id)
                sqls = client.list_sql(app_id, length=100000)
                sections = fmt.format_summary(app, env, jobs, stages, executors, sqls)
                for title, info in sections.items():
                    output_status_block(skin, info, title=title)

            elif cmd == "jobs":
                app_id = state.resolve_app_id(args[0] if args else None)
                status_filter = None
                if "--status" in args:
                    idx = args.index("--status")
                    if idx + 1 < len(args):
                        status_filter = args[idx + 1]
                jobs = client.list_jobs(app_id, status=status_filter)
                headers, rows = fmt.format_job_list(jobs)
                output_table(skin, headers, rows)

            elif cmd == "job":
                if not args:
                    skin.error("Usage: job <job-id>")
                else:
                    app_id = state.session.require_app()
                    job = client.get_job(app_id, int(args[0]))
                    info = fmt.format_job_detail(job)
                    output_status_block(skin, info, title=f"Job {args[0]}")

            elif cmd == "job-stages":
                if not args or not args[0].isdigit():
                    skin.error("Usage: job-stages <job-id>")
                else:
                    app_id = state.session.require_app()
                    job_id = int(args[0])
                    job = client.get_job(app_id, job_id)
                    stage_ids = job.get("stageIds", [])
                    if not stage_ids:
                        skin.warning(f"No stages found for job {job_id}")
                    else:
                        all_stages = client.list_stages(app_id)
                        stages = [s for s in all_stages if s.get("stageId") in stage_ids]
                        if not stages:
                            skin.warning(f"Job {job_id} references stages {stage_ids} but none were found")
                        else:
                            skin.section(f"Stages for Job {job_id} ({len(stages)}/{len(stage_ids)} stages)")
                            headers, rows = fmt.format_stage_list(stages)
                            output_table(skin, headers, rows)

            elif cmd == "stages":
                app_id = state.resolve_app_id(None)
                status_filter = None
                if "--status" in args:
                    idx = args.index("--status")
                    if idx + 1 < len(args):
                        status_filter = args[idx + 1]
                stages = client.list_stages(app_id, status=status_filter)
                headers, rows = fmt.format_stage_list(stages)
                output_table(skin, headers, rows)

            elif cmd == "stage":
                if not args:
                    skin.error("Usage: stage <stage-id> [attempt-id]")
                else:
                    app_id = state.session.require_app()
                    stage_id = int(args[0])
                    if len(args) > 1:
                        stage = client.get_stage_attempt(app_id, stage_id, int(args[1]))
                        info = fmt.format_stage_detail(stage)
                        output_status_block(skin, info, title=f"Stage {stage_id}/{args[1]}")
                    else:
                        stages = client.get_stage(app_id, stage_id)
                        if len(stages) == 1:
                            info = fmt.format_stage_detail(stages[0])
                            output_status_block(skin, info, title=f"Stage {stage_id}")
                        else:
                            headers, rows = fmt.format_stage_list(stages)
                            output_table(skin, headers, rows)

            elif cmd == "stage-summary":
                if not args or not args[0].isdigit():
                    skin.error("Usage: stage-summary <stage-id> [attempt-id]")
                else:
                    app_id = state.session.require_app()
                    stage_id = int(args[0])
                    attempt_id = int(args[1]) if len(args) > 1 and args[1].isdigit() else 0
                    data = client.get_task_summary(app_id, stage_id, attempt_id)
                    headers, rows = fmt.format_task_summary(data)
                    skin.section(f"Task Summary for Stage {stage_id}/{attempt_id}")
                    output_table(skin, headers, rows)

            elif cmd == "stage-tasks":
                if not args or not args[0].isdigit():
                    skin.error("Usage: stage-tasks <stage-id> [attempt-id] [--length N] [--sort-by FIELD]")
                else:
                    app_id = state.session.require_app()
                    stage_id = int(args[0])
                    attempt_id = int(args[1]) if len(args) > 1 and args[1].isdigit() else 0
                    length = 20
                    sort_by = "ID"
                    for i, a in enumerate(args):
                        if a == "--length" and i + 1 < len(args):
                            length = int(args[i + 1])
                        if a == "--sort-by" and i + 1 < len(args):
                            sort_by = args[i + 1]
                    data = client.list_tasks(app_id, stage_id, attempt_id, length=length, sort_by=sort_by)
                    headers, rows = fmt.format_task_list(data)
                    skin.section(f"Tasks for Stage {stage_id}/{attempt_id}")
                    output_table(skin, headers, rows)

            elif cmd in ("executors", "execs"):
                app_id = state.resolve_app_id(None)
                all_flag = "--all" in args
                if all_flag:
                    execs = client.list_all_executors(app_id)
                else:
                    execs = client.list_executors(app_id)
                headers, rows = fmt.format_executor_list(execs)
                output_table(skin, headers, rows)

            elif cmd == "sql":
                app_id = state.resolve_app_id(None)
                if args and args[0].isdigit():
                    ex = client.get_sql(app_id, int(args[0]))
                    info = fmt.format_sql_detail(ex)
                    output_status_block(skin, info, title=f"SQL Execution {args[0]}")
                else:
                    sqls = client.list_sql(app_id)
                    headers, rows = fmt.format_sql_list(sqls)
                    output_table(skin, headers, rows)

            elif cmd == "sql-plan":
                app_id = state.resolve_app_id(None)
                if not args or not args[0].isdigit():
                    skin.error("Usage: sql-plan <execution-id> [--view initial|final|full] [--dot]")
                else:
                    exec_id = int(args[0])
                    view = "full"
                    dot_mode = "--dot" in args
                    for i, a in enumerate(args[1:], 1):
                        if a == "--view" and i + 1 < len(args):
                            view = args[i + 1]
                    ex = client.get_sql(app_id, exec_id)
                    if dot_mode:
                        dot = fmt.plan_to_dot(
                            ex.get("nodes", []),
                            ex.get("edges", []),
                            graph_name=f"SQL {exec_id}",
                        )
                        click.echo(dot)
                    else:
                        parsed = fmt.parse_plan_sections(ex.get("planDescription", ""))
                        if view == "initial":
                            click.echo(parsed["initialPlan"])
                        elif view == "final":
                            click.echo(parsed["finalPlan"])
                        else:
                            click.echo(parsed["fullPlan"])

            elif cmd == "sql-jobs":
                app_id = state.resolve_app_id(None)
                if not args or not args[0].isdigit():
                    skin.error("Usage: sql-jobs <execution-id>")
                else:
                    exec_id = int(args[0])
                    ex = client.get_sql(app_id, exec_id)
                    job_ids = _collect_sql_job_ids(ex)
                    if not job_ids:
                        skin.warning(f"No jobs found for SQL execution {exec_id}")
                    else:
                        jobs = _fetch_sql_jobs(client, app_id, ex)
                        if not jobs:
                            skin.warning(f"SQL execution {exec_id} references jobs {job_ids} but none were found")
                        else:
                            skin.section(f"Jobs for SQL Execution {exec_id} ({len(jobs)}/{len(job_ids)} jobs)")
                            headers, rows = fmt.format_job_list(jobs)
                            output_table(skin, headers, rows)

            elif cmd == "rdds":
                app_id = state.resolve_app_id(None)
                rdds = client.list_rdds(app_id)
                headers, rows = fmt.format_rdd_list(rdds)
                output_table(skin, headers, rows)

            elif cmd == "rdd":
                if not args or not args[0].isdigit():
                    skin.error("Usage: rdd <rdd-id>")
                else:
                    app_id = state.session.require_app()
                    data = client.get_rdd(app_id, int(args[0]))
                    info = fmt.format_rdd_detail(data)
                    output_status_block(skin, info, title=f"RDD {args[0]}")

            elif cmd == "env":
                app_id = state.resolve_app_id(None)
                env = client.get_environment(app_id)
                runtime = fmt.format_environment(env)
                output_status_block(skin, runtime, title="Runtime")
                skin.section("Spark Properties")
                headers, rows = fmt.format_spark_properties(env)
                output_table(skin, headers, rows)

            elif cmd == "logs":
                app_id = state.resolve_app_id(None)
                out = args[0] if args else f"eventLogs-{app_id}.zip"
                path = client.download_logs(app_id, out)
                skin.success(f"Event logs saved to {path}")

            else:
                skin.warning(f"Unknown command: {cmd}. Type 'help' for available commands.")

        except ValueError as e:
            skin.error(str(e))
        except HistoryServerError as e:
            skin.error(str(e))
        except Exception as e:
            skin.error(f"Unexpected error: {e}")


# ── Subcommands (one-shot mode) ───────────────────────────────────────

@cli.command("version")
@pass_state
def cmd_version(state: CliState):
    """Show Spark History Server version."""
    client = state.ensure_client()
    ver = client.get_version()
    if state.json_mode:
        output_json(ver)
    else:
        click.echo(f"Spark {ver.get('spark', 'unknown')}")


@cli.command("apps")
@click.option("--status", type=click.Choice(["completed", "running"], case_sensitive=False))
@click.option("--limit", type=int, default=None)
@click.option("--min-date", default=None, help="Min start date (ISO format)")
@click.option("--max-date", default=None, help="Max start date (ISO format)")
@pass_state
def cmd_apps(state: CliState, status, limit, min_date, max_date):
    """List applications."""
    client = state.ensure_client()
    apps = client.list_applications(
        status=status, limit=limit, min_date=min_date, max_date=max_date,
    )
    if state.json_mode:
        output_json(apps)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        headers, rows = fmt.format_app_list(apps)
        output_table(skin, headers, rows)


@cli.command("app")
@click.argument("app_id")
@pass_state
def cmd_app(state: CliState, app_id: str):
    """Show application details."""
    client = state.ensure_client()
    app = client.get_application(app_id)
    if state.json_mode:
        output_json(app)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        info = fmt.format_app_detail(app)
        output_status_block(skin, info, title="Application")


@cli.command("attempts")
@pass_state
def cmd_attempts(state: CliState):
    """List attempts for an application.

    Examples:

      spark-history-cli -a <app> attempts
    """
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    app = client.get_application(app_id)
    attempts = app.get("attempts", [])
    if state.json_mode:
        output_json(attempts)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        headers, rows = fmt.format_attempt_list(attempts)
        output_table(skin, headers, rows)


@cli.command("attempt")
@click.argument("attempt_id")
@pass_state
def cmd_attempt(state: CliState, attempt_id: str):
    """Show details for a specific application attempt.

    Examples:

      spark-history-cli -a <app> attempt 1
    """
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    data = client.get_attempt(app_id, attempt_id)
    if state.json_mode:
        output_json(data)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        info = fmt.format_attempt_detail(data)
        output_status_block(skin, info, title=f"Attempt {attempt_id}")


@cli.command("processes")
@pass_state
def cmd_processes(state: CliState):
    """List miscellaneous processes for an application.

    Shows streaming receivers and other auxiliary processes.

    Examples:

      spark-history-cli -a <app> processes
    """
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    procs = client.list_misc_processes(app_id)
    if state.json_mode:
        output_json(procs)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        headers, rows = fmt.format_process_list(procs)
        output_table(skin, headers, rows)


@cli.command("summary")
@pass_state
def cmd_summary(state: CliState):
    """Show a concise summary of an application.

    Aggregates application details, resource config, and workload stats
    into a single overview.

    Examples:

      spark-history-cli -a <app> summary

      spark-history-cli -a <app> --json summary
    """
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    app = client.get_application(app_id)
    env = client.get_environment(app_id)
    jobs = client.list_jobs(app_id)
    stages = client.list_stages(app_id)
    executors = client.list_all_executors(app_id)
    sqls = client.list_sql(app_id, length=100000)
    if state.json_mode:
        sections = fmt.format_summary(app, env, jobs, stages, executors, sqls)
        output_json(sections)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        sections = fmt.format_summary(app, env, jobs, stages, executors, sqls)
        for title, info in sections.items():
            output_status_block(skin, info, title=title)


@cli.command("jobs")
@click.option("--status", type=click.Choice(
    ["running", "succeeded", "failed", "unknown"], case_sensitive=False))
@pass_state
def cmd_jobs(state: CliState, status):
    """List jobs for an application."""
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    jobs = client.list_jobs(app_id, status=status)
    if state.json_mode:
        output_json(jobs)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        headers, rows = fmt.format_job_list(jobs)
        output_table(skin, headers, rows)


@cli.command("job")
@click.argument("job_id", type=int)
@pass_state
def cmd_job(state: CliState, job_id: int):
    """Show job details."""
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    job = client.get_job(app_id, job_id)
    if state.json_mode:
        output_json(job)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        info = fmt.format_job_detail(job)
        output_status_block(skin, info, title=f"Job {job_id}")


@cli.command("job-stages")
@click.argument("job_id", type=int)
@pass_state
def cmd_job_stages(state: CliState, job_id: int):
    """Show stages belonging to a job.

    Fetches the job to get its stage IDs, then retrieves and displays
    each stage's details.

    Examples:

      spark-history-cli -a <app> job-stages 42

      spark-history-cli -a <app> --json job-stages 42
    """
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    job = client.get_job(app_id, job_id)
    stage_ids = job.get("stageIds", [])
    if not stage_ids:
        click.echo(f"No stages found for job {job_id}.")
        return
    all_stages = client.list_stages(app_id)
    stages = [s for s in all_stages if s.get("stageId") in stage_ids]
    if state.json_mode:
        output_json(stages)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        skin.section(f"Stages for Job {job_id} ({len(stages)}/{len(stage_ids)} stages)")
        headers, rows = fmt.format_stage_list(stages)
        output_table(skin, headers, rows)


@cli.command("stages")
@click.option("--status", type=click.Choice(
    ["active", "complete", "pending", "failed"], case_sensitive=False))
@pass_state
def cmd_stages(state: CliState, status):
    """List stages for an application."""
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    stages = client.list_stages(app_id, status=status)
    if state.json_mode:
        output_json(stages)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        headers, rows = fmt.format_stage_list(stages)
        output_table(skin, headers, rows)


@cli.command("stage")
@click.argument("stage_id", type=int)
@click.option("--attempt", type=int, default=None)
@pass_state
def cmd_stage(state: CliState, stage_id: int, attempt: int | None):
    """Show stage details."""
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    if attempt is not None:
        stage = client.get_stage_attempt(app_id, stage_id, attempt)
        data = stage
    else:
        stages = client.get_stage(app_id, stage_id)
        data = stages[0] if len(stages) == 1 else stages
    if state.json_mode:
        output_json(data)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        if isinstance(data, list):
            headers, rows = fmt.format_stage_list(data)
            output_table(skin, headers, rows)
        else:
            info = fmt.format_stage_detail(data)
            output_status_block(skin, info, title=f"Stage {stage_id}")


@cli.command("stage-summary")
@click.argument("stage_id", type=int)
@click.option("--attempt", type=int, default=0, show_default=True,
              help="Stage attempt ID.")
@pass_state
def cmd_stage_summary(state: CliState, stage_id: int, attempt: int):
    """Show task metric quantiles for a stage (p5/p25/p50/p75/p95).

    Examples:

      spark-history-cli -a <app> stage-summary 42

      spark-history-cli -a <app> stage-summary 42 --attempt 1
    """
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    data = client.get_task_summary(app_id, stage_id, attempt)
    if state.json_mode:
        output_json(data)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        headers, rows = fmt.format_task_summary(data)
        skin.section(f"Task Summary for Stage {stage_id}/{attempt}")
        output_table(skin, headers, rows)


@cli.command("stage-tasks")
@click.argument("stage_id", type=int)
@click.option("--attempt", type=int, default=0, show_default=True,
              help="Stage attempt ID.")
@click.option("--offset", type=int, default=0, help="Offset for pagination.")
@click.option("--length", type=int, default=20, show_default=True,
              help="Number of tasks to return.")
@click.option("--sort-by", type=click.Choice(
    ["ID", "runtime", "-runtime"],
    case_sensitive=True), default="-runtime", show_default=True,
    help="Sort field (use - prefix for descending).")
@pass_state
def cmd_stage_tasks(state: CliState, stage_id: int, attempt: int,
                    offset: int, length: int, sort_by: str):
    """List individual tasks for a stage.

    Examples:

      spark-history-cli -a <app> stage-tasks 42

      spark-history-cli -a <app> stage-tasks 42 --sort-by -runtime --length 10
    """
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    data = client.list_tasks(app_id, stage_id, attempt,
                             offset=offset, length=length,
                             sort_by=sort_by)
    if state.json_mode:
        output_json(data)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        headers, rows = fmt.format_task_list(data)
        skin.section(f"Tasks for Stage {stage_id}/{attempt} (offset={offset}, limit={length})")
        output_table(skin, headers, rows)


@cli.command("executors")
@click.option("--all", "show_all", is_flag=True, help="Include dead executors")
@pass_state
def cmd_executors(state: CliState, show_all: bool):
    """List executors for an application."""
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    execs = client.list_all_executors(app_id) if show_all else client.list_executors(app_id)
    if state.json_mode:
        output_json(execs)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        headers, rows = fmt.format_executor_list(execs)
        output_table(skin, headers, rows)


@cli.command("sql")
@click.argument("execution_id", type=int, required=False, default=None)
@click.option("--offset", type=int, default=0)
@click.option("--length", type=int, default=20)
@pass_state
def cmd_sql(state: CliState, execution_id: int | None, offset: int, length: int):
    """List or show SQL executions."""
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    if execution_id is not None:
        data = client.get_sql(app_id, execution_id)
        if state.json_mode:
            output_json(data)
        else:
            from spark_history_cli.utils.repl_skin import ReplSkin
            skin = ReplSkin("spark_history", version=__version__)
            info = fmt.format_sql_detail(data)
            output_status_block(skin, info, title=f"SQL Execution {execution_id}")
    else:
        data = client.list_sql(app_id, offset=offset, length=length)
        if state.json_mode:
            output_json(data)
        else:
            from spark_history_cli.utils.repl_skin import ReplSkin
            skin = ReplSkin("spark_history", version=__version__)
            headers, rows = fmt.format_sql_list(data)
            output_table(skin, headers, rows)


@cli.command("sql-plan")
@click.argument("execution_id", type=int)
@click.option("--view", "view_mode", type=click.Choice(["full", "initial", "final"], case_sensitive=False),
              default="full", show_default=True, help="Which plan view to show.")
@click.option("--dot", "dot_mode", is_flag=True, default=False,
              help="Output as Graphviz DOT instead of text plan.")
@click.option("--output", "-o", "output_file", default=None,
              help="Write output to a file instead of stdout.")
@pass_state
def cmd_sql_plan(state: CliState, execution_id: int, view_mode: str, dot_mode: bool, output_file: str | None):
    """Show SQL execution plan (text or Graphviz DOT).

    Examples:

      spark-history-cli -a <app> sql-plan 4 --view final

      spark-history-cli -a <app> sql-plan 4 --dot -o plan.dot

      spark-history-cli -a <app> --json sql-plan 4
    """
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    ex = client.get_sql(app_id, execution_id)

    if dot_mode:
        content = fmt.plan_to_dot(
            ex.get("nodes", []),
            ex.get("edges", []),
            graph_name=f"SQL {execution_id}",
        )
        if output_file:
            Path(output_file).write_text(content, encoding="utf-8")
            click.echo(f"DOT file written to {output_file}")
        else:
            click.echo(content)
        return

    parsed = fmt.parse_plan_sections(ex.get("planDescription", ""))

    if state.json_mode:
        result = {
            "executionId": execution_id,
            "isAdaptive": parsed["isAdaptive"],
            "sectionCount": len(parsed["sections"]),
        }
        if view_mode == "initial":
            result["plan"] = parsed["initialPlan"]
        elif view_mode == "final":
            result["plan"] = parsed["finalPlan"]
        else:
            result["plan"] = parsed["fullPlan"]
            result["sections"] = parsed["sections"]
        output_json(result)
    else:
        if view_mode == "initial":
            text = parsed["initialPlan"]
        elif view_mode == "final":
            text = parsed["finalPlan"]
        else:
            text = parsed["fullPlan"]

        if output_file:
            Path(output_file).write_text(text, encoding="utf-8")
            click.echo(f"Plan written to {output_file}")
        else:
            click.echo(text)


@cli.command("sql-jobs")
@click.argument("execution_id", type=int)
@pass_state
def cmd_sql_jobs(state: CliState, execution_id: int):
    """Show jobs associated with a SQL execution.

    Fetches the SQL execution, collects all job IDs (succeeded, failed,
    running), and displays each job's details.

    Examples:

      spark-history-cli -a <app> sql-jobs 4

      spark-history-cli -a <app> --json sql-jobs 4
    """
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    ex = client.get_sql(app_id, execution_id)
    job_ids = _collect_sql_job_ids(ex)
    if not job_ids:
        click.echo(f"No jobs found for SQL execution {execution_id}.")
        return
    jobs = _fetch_sql_jobs(client, app_id, ex)
    if not jobs:
        click.echo(f"SQL execution {execution_id} references jobs {job_ids} but none were found.")
        return
    if state.json_mode:
        output_json(jobs)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        skin.section(f"Jobs for SQL Execution {execution_id} ({len(jobs)}/{len(job_ids)} jobs)")
        headers, rows = fmt.format_job_list(jobs)
        output_table(skin, headers, rows)


@cli.command("rdds")
@pass_state
def cmd_rdds(state: CliState):
    """List cached RDDs for an application."""
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    rdds = client.list_rdds(app_id)
    if state.json_mode:
        output_json(rdds)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        headers, rows = fmt.format_rdd_list(rdds)
        output_table(skin, headers, rows)


@cli.command("rdd")
@click.argument("rdd_id", type=int)
@pass_state
def cmd_rdd(state: CliState, rdd_id: int):
    """Show details for a specific cached RDD.

    Examples:

      spark-history-cli -a <app> rdd 42
    """
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    data = client.get_rdd(app_id, rdd_id)
    if state.json_mode:
        output_json(data)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        info = fmt.format_rdd_detail(data)
        output_status_block(skin, info, title=f"RDD {rdd_id}")


@cli.command("env")
@pass_state
def cmd_env(state: CliState):
    """Show application environment and Spark configuration."""
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    env = client.get_environment(app_id)
    if state.json_mode:
        output_json(env)
    else:
        from spark_history_cli.utils.repl_skin import ReplSkin
        skin = ReplSkin("spark_history", version=__version__)
        runtime = fmt.format_environment(env)
        output_status_block(skin, runtime, title="Runtime")
        skin.section("Spark Properties")
        headers, rows = fmt.format_spark_properties(env)
        output_table(skin, headers, rows)


@cli.command("logs")
@click.argument("output_path", required=False, default=None)
@pass_state
def cmd_logs(state: CliState, output_path: str | None):
    """Download event logs as a ZIP file."""
    client = state.ensure_client()
    app_id = state.resolve_app_id(None)
    out = output_path or f"eventLogs-{app_id}.zip"
    path = client.download_logs(app_id, out)
    if state.json_mode:
        output_json({"output": path, "app_id": app_id})
    else:
        click.echo(f"Event logs saved to {path}")


@cli.command("install-skill")
@click.option(
    "--scope",
    type=click.Choice(["user", "repo"], case_sensitive=False),
    default="user",
    show_default=True,
    help="Install as a personal skill (~/.copilot/skills) or repository skill (.github/skills).",
)
@click.option(
    "--target-dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    default=None,
    help="Install to a specific skill directory instead of the default location.",
)
@click.option("--force", is_flag=True, help="Overwrite an existing skill directory.")
@pass_state
def cmd_install_skill(
    state: CliState,
    scope: str,
    target_dir: Path | None,
    force: bool,
):
    """Install the bundled Copilot skill."""
    destination = target_dir or default_skill_target(scope)
    try:
        installed_path = install_copilot_skill(destination, force=force)
    except FileExistsError as exc:
        raise click.ClickException(str(exc)) from exc

    result = {
        "name": "spark-history-cli",
        "installed_to": str(installed_path),
        "scope": scope,
        "next_steps": [
            "Run /skills reload in Copilot CLI if it is already open.",
            "Verify with /skills list or /skills info spark-history-cli.",
            "Use it with prompts like 'Use /spark-history-cli to inspect my latest SHS app'.",
        ],
    }
    if state.json_mode:
        output_json(result)
    else:
        click.echo(f"Installed Copilot skill to {installed_path}")
        click.echo("Next steps:")
        for step in result["next_steps"]:
            click.echo(f"  - {step}")


# ── Entry point ───────────────────────────────────────────────────────

def main():
    cli(auto_envvar_prefix="SPARK_HISTORY")


if __name__ == "__main__":
    main()
