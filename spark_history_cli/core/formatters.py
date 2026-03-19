"""Terminal formatters for Spark History Server data.

Formats API responses as human-readable tables and summaries.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _ts(epoch_ms: int | None) -> str:
    """Format epoch milliseconds to human-readable datetime string."""
    if not epoch_ms:
        return "N/A"
    try:
        dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except (OSError, ValueError):
        return "N/A"


def _ts_from_str(date_str: str | None) -> str:
    """Format an ISO date string to a shorter form."""
    if not date_str:
        return "N/A"
    return date_str.replace("T", " ").split(".")[0] if "T" in date_str else date_str


def _duration(ms: int | None) -> str:
    """Format duration in milliseconds to human-readable string."""
    if ms is None or ms < 0:
        return "N/A"
    if ms < 1000:
        return f"{ms}ms"
    secs = ms / 1000
    if secs < 60:
        return f"{secs:.1f}s"
    mins = secs / 60
    if mins < 60:
        return f"{int(mins)}m {int(secs % 60)}s"
    hours = mins / 60
    return f"{int(hours)}h {int(mins % 60)}m"


def _bytes(b: int | None) -> str:
    """Format bytes to human-readable string."""
    if b is None or b < 0:
        return "N/A"
    if b < 1024:
        return f"{b} B"
    if b < 1024 * 1024:
        return f"{b / 1024:.1f} KB"
    if b < 1024 * 1024 * 1024:
        return f"{b / (1024 * 1024):.1f} MB"
    return f"{b / (1024 * 1024 * 1024):.2f} GB"


def _status_icon(status: str) -> str:
    """Return a status icon for common statuses."""
    s = status.upper() if status else ""
    icons = {
        "SUCCEEDED": "✓",
        "COMPLETED": "✓",
        "COMPLETE": "✓",
        "RUNNING": "▶",
        "FAILED": "✗",
        "KILLED": "⊘",
        "PENDING": "○",
        "ACTIVE": "●",
        "SKIPPED": "⊘",
        "UNKNOWN": "?",
    }
    return icons.get(s, " ")


# ── Application Formatters ────────────────────────────────────────────


def format_app_list(apps: list[dict]) -> tuple[list[str], list[list[str]]]:
    """Format application list as table headers and rows."""
    headers = ["App ID", "Name", "Status", "Started", "Duration", "User"]
    rows = []
    for app in apps:
        attempts = app.get("attempts", [])
        latest = attempts[0] if attempts else {}
        status = "RUNNING" if not latest.get("completed", True) else "COMPLETED"
        rows.append([
            app.get("id", ""),
            app.get("name", "")[:40],
            f"{_status_icon(status)} {status}",
            _ts(latest.get("startTimeEpoch")),
            _duration(latest.get("duration")),
            latest.get("sparkUser", ""),
        ])
    return headers, rows


def format_app_detail(app: dict) -> dict[str, str]:
    """Format application detail as a status block."""
    attempts = app.get("attempts", [])
    latest = attempts[0] if attempts else {}
    status = "RUNNING" if not latest.get("completed", True) else "COMPLETED"
    info = {
        "App ID": app.get("id", ""),
        "Name": app.get("name", ""),
        "Status": f"{_status_icon(status)} {status}",
        "User": latest.get("sparkUser", ""),
        "Started": _ts(latest.get("startTimeEpoch")),
        "Ended": _ts(latest.get("endTimeEpoch")),
        "Duration": _duration(latest.get("duration")),
        "Spark Version": latest.get("appSparkVersion", ""),
        "Attempts": str(len(attempts)),
    }
    return info


# ── Job Formatters ────────────────────────────────────────────────────


def format_job_list(jobs: list[dict]) -> tuple[list[str], list[list[str]]]:
    """Format job list as table headers and rows."""
    headers = ["Job ID", "Name", "Status", "Stages", "Tasks (done/total)"]
    rows = []
    for job in jobs:
        status = job.get("status", "UNKNOWN")
        completed = job.get("numCompletedStages", 0)
        total_stages = (
            completed
            + job.get("numActiveStages", 0)
            + job.get("numFailedStages", 0)
            + job.get("numSkippedStages", 0)
        )
        rows.append([
            str(job.get("jobId", "")),
            (job.get("name") or job.get("description") or "")[:50],
            f"{_status_icon(status)} {status}",
            f"{completed}/{total_stages}",
            f"{job.get('numCompletedTasks', 0)}/{job.get('numTasks', 0)}",
        ])
    return headers, rows


def format_job_detail(job: dict) -> dict[str, str]:
    """Format job detail as a status block."""
    status = job.get("status", "UNKNOWN")
    return {
        "Job ID": str(job.get("jobId", "")),
        "Name": job.get("name", ""),
        "Description": job.get("description") or "N/A",
        "Status": f"{_status_icon(status)} {status}",
        "Submitted": _ts_from_str(job.get("submissionTime")),
        "Completed": _ts_from_str(job.get("completionTime")),
        "Stages (active/done/failed/skipped)": (
            f"{job.get('numActiveStages', 0)}/"
            f"{job.get('numCompletedStages', 0)}/"
            f"{job.get('numFailedStages', 0)}/"
            f"{job.get('numSkippedStages', 0)}"
        ),
        "Tasks (active/done/failed/killed)": (
            f"{job.get('numActiveTasks', 0)}/"
            f"{job.get('numCompletedTasks', 0)}/"
            f"{job.get('numFailedTasks', 0)}/"
            f"{job.get('numKilledTasks', 0)}"
        ),
        "Stage IDs": ", ".join(str(s) for s in job.get("stageIds", [])),
    }


# ── Stage Formatters ──────────────────────────────────────────────────


def format_stage_list(stages: list[dict]) -> tuple[list[str], list[list[str]]]:
    """Format stage list as table headers and rows."""
    headers = ["Stage ID", "Attempt", "Name", "Status", "Tasks", "Input", "Output", "Shuffle R/W"]
    rows = []
    for s in stages:
        status = s.get("status", "UNKNOWN")
        rows.append([
            str(s.get("stageId", "")),
            str(s.get("attemptId", 0)),
            (s.get("name") or "")[:35],
            f"{_status_icon(status)} {status}",
            f"{s.get('numCompleteTasks', 0)}/{s.get('numTasks', 0)}",
            _bytes(s.get("inputBytes")),
            _bytes(s.get("outputBytes")),
            f"{_bytes(s.get('shuffleReadBytes'))}/{_bytes(s.get('shuffleWriteBytes'))}",
        ])
    return headers, rows


def format_stage_detail(stage: dict) -> dict[str, str]:
    """Format stage detail as a status block."""
    status = stage.get("status", "UNKNOWN")
    return {
        "Stage ID": str(stage.get("stageId", "")),
        "Attempt": str(stage.get("attemptId", 0)),
        "Name": stage.get("name", ""),
        "Status": f"{_status_icon(status)} {status}",
        "Tasks (active/done/failed/killed)": (
            f"{stage.get('numActiveTasks', 0)}/"
            f"{stage.get('numCompleteTasks', 0)}/"
            f"{stage.get('numFailedTasks', 0)}/"
            f"{stage.get('numKilledTasks', 0)}"
        ),
        "Input": _bytes(stage.get("inputBytes")),
        "Output": _bytes(stage.get("outputBytes")),
        "Shuffle Read": _bytes(stage.get("shuffleReadBytes")),
        "Shuffle Write": _bytes(stage.get("shuffleWriteBytes")),
        "Executor Run Time": _duration(stage.get("executorRunTime")),
        "JVM GC Time": _duration(stage.get("jvmGcTime")),
        "Memory Spilled": _bytes(stage.get("memoryBytesSpilled")),
        "Disk Spilled": _bytes(stage.get("diskBytesSpilled")),
    }


# ── Executor Formatters ───────────────────────────────────────────────


def format_executor_list(executors: list[dict]) -> tuple[list[str], list[list[str]]]:
    """Format executor list as table headers and rows."""
    headers = ["ID", "Host:Port", "Active", "Cores", "Tasks (done/total)", "Memory", "Disk"]
    rows = []
    for ex in executors:
        rows.append([
            ex.get("id", ""),
            ex.get("hostPort", ""),
            "✓" if ex.get("isActive") else "✗",
            str(ex.get("totalCores", 0)),
            f"{ex.get('completedTasks', 0)}/{ex.get('totalTasks', 0)}",
            f"{_bytes(ex.get('memoryUsed'))}/{_bytes(ex.get('maxMemory'))}",
            _bytes(ex.get("diskUsed")),
        ])
    return headers, rows


# ── SQL Formatters ────────────────────────────────────────────────────


def format_sql_list(executions: list[dict]) -> tuple[list[str], list[list[str]]]:
    """Format SQL execution list as table headers and rows."""
    headers = ["Exec ID", "Status", "Description", "Duration", "Jobs (ok/fail/run)"]
    rows = []
    for ex in executions:
        rows.append([
            str(ex.get("id", "")),
            ex.get("status", ""),
            (ex.get("description") or "")[:50],
            _duration(ex.get("duration")),
            (
                f"{len(ex.get('successJobIds', []))}/"
                f"{len(ex.get('failedJobIds', []))}/"
                f"{len(ex.get('runningJobIds', []))}"
            ),
        ])
    return headers, rows


def format_sql_detail(ex: dict) -> dict[str, str]:
    """Format SQL execution detail as a status block."""
    return {
        "Execution ID": str(ex.get("id", "")),
        "Status": ex.get("status", ""),
        "Description": ex.get("description") or "N/A",
        "Duration": _duration(ex.get("duration")),
        "Submitted": _ts_from_str(ex.get("submissionTime")),
        "Running Jobs": ", ".join(str(j) for j in ex.get("runningJobIds", [])) or "None",
        "Succeeded Jobs": ", ".join(str(j) for j in ex.get("successJobIds", [])) or "None",
        "Failed Jobs": ", ".join(str(j) for j in ex.get("failedJobIds", [])) or "None",
        "Error": ex.get("errorMessage") or "None",
    }


# ── RDD Storage Formatters ────────────────────────────────────────────


def format_rdd_list(rdds: list[dict]) -> tuple[list[str], list[list[str]]]:
    """Format RDD storage list as table headers and rows."""
    headers = ["RDD ID", "Name", "Partitions (cached/total)", "Storage Level", "Memory", "Disk"]
    rows = []
    for r in rdds:
        rows.append([
            str(r.get("id", "")),
            (r.get("name") or "")[:35],
            f"{r.get('numCachedPartitions', 0)}/{r.get('numPartitions', 0)}",
            r.get("storageLevel", ""),
            _bytes(r.get("memoryUsed")),
            _bytes(r.get("diskUsed")),
        ])
    return headers, rows


# ── SQL Plan Formatters ───────────────────────────────────────────────


def parse_plan_sections(plan_text: str) -> dict:
    """Parse a planDescription into structured sections.

    Returns a dict with keys: fullPlan, initialPlan, finalPlan, sections, isAdaptive.
    """
    if not plan_text:
        return {
            "fullPlan": "",
            "initialPlan": "",
            "finalPlan": "",
            "sections": [],
            "isAdaptive": False,
        }

    import re
    lines = plan_text.split("\n")
    sections: list[dict] = []
    current_kind: str | None = None
    current_lines: list[str] = []
    initial_ordinal = 0
    final_ordinal = 0

    for line in lines:
        if re.match(r"^\+?-?\s*==\s*Initial Plan\s*==", line):
            if current_kind and current_lines:
                sections.append({"kind": current_kind, "ordinal": initial_ordinal if current_kind == "initial" else final_ordinal, "text": "\n".join(current_lines)})
            initial_ordinal += 1
            current_kind = "initial"
            current_lines = [line]
        elif re.match(r"^\+?-?\s*==\s*Final Plan\s*==", line):
            if current_kind and current_lines:
                sections.append({"kind": current_kind, "ordinal": initial_ordinal if current_kind == "initial" else final_ordinal, "text": "\n".join(current_lines)})
            final_ordinal += 1
            current_kind = "final"
            current_lines = [line]
        elif current_kind:
            current_lines.append(line)

    if current_kind and current_lines:
        sections.append({"kind": current_kind, "ordinal": initial_ordinal if current_kind == "initial" else final_ordinal, "text": "\n".join(current_lines)})

    is_adaptive = any(s["kind"] == "initial" for s in sections) or any(s["kind"] == "final" for s in sections)

    initial_texts = [s["text"] for s in sections if s["kind"] == "initial"]
    final_texts = [s["text"] for s in sections if s["kind"] == "final"]

    return {
        "fullPlan": plan_text,
        "initialPlan": "\n\n".join(initial_texts) if initial_texts else plan_text,
        "finalPlan": "\n\n".join(final_texts) if final_texts else plan_text,
        "sections": sections,
        "isAdaptive": is_adaptive,
    }


def plan_to_dot(nodes: list[dict], edges: list[dict],
                graph_name: str = "SparkPlan") -> str:
    """Convert SHS SQL nodes/edges into a Graphviz DOT string."""
    lines = [f'digraph "{graph_name}" {{']
    lines.append('  rankdir=TB;')
    lines.append('  node [shape=box, style="rounded,filled", fillcolor="#e8f4fd", fontname="Helvetica", fontsize=10];')
    lines.append('  edge [color="#666666"];')
    lines.append("")

    for node in nodes:
        nid = node.get("nodeId", 0)
        name = node.get("nodeName", f"node_{nid}")
        # Escape quotes
        label = name.replace('"', '\\"')
        # Truncate very long labels
        if len(label) > 80:
            label = label[:77] + "..."
        lines.append(f'  n{nid} [label="{label} (#{nid})"];')

    lines.append("")
    for edge in edges:
        lines.append(f'  n{edge["fromId"]} -> n{edge["toId"]};')

    lines.append("}")
    return "\n".join(lines)


# ── Environment Formatters ────────────────────────────────────────────


def format_environment(env: dict) -> dict[str, str]:
    """Format environment info as a status block."""
    runtime = env.get("runtime", {})
    return {
        "Java Version": runtime.get("javaVersion", ""),
        "Java Home": runtime.get("javaHome", ""),
        "Scala Version": runtime.get("scalaVersion", ""),
    }


def format_spark_properties(env: dict) -> tuple[list[str], list[list[str]]]:
    """Format spark properties as table."""
    headers = ["Property", "Value"]
    props = env.get("sparkProperties", [])
    rows = [[p[0], str(p[1])[:80]] for p in props]
    return headers, rows
