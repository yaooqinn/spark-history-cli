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


def format_summary(
    app: dict,
    env: dict,
    jobs: list[dict],
    stages: list[dict],
    executors: list[dict],
    sqls: list[dict],
) -> dict[str, dict[str, str]]:
    """Build a multi-section summary from several API responses.

    Returns an ordered dict of {section_title: {key: value}} pairs.
    """
    from collections import Counter

    attempts = app.get("attempts", [])
    latest = attempts[0] if attempts else {}
    status = "RUNNING" if not latest.get("completed", True) else "COMPLETED"
    runtime = env.get("runtime", {})
    sp = dict(env.get("sparkProperties", []))

    # ── Application ──
    application = {
        "App ID": app.get("id", ""),
        "Name": app.get("name", ""),
        "Status": f"{_status_icon(status)} {status}",
        "Duration": _duration(latest.get("duration")),
        "Spark Version": (
            f"{latest.get('appSparkVersion', 'N/A')}  "
            f"(Scala {runtime.get('scalaVersion', 'N/A').replace('version ', '')}, "
            f"Java {runtime.get('javaVersion', 'N/A')})"
        ),
        "Master": sp.get("spark.master", "N/A"),
        "User": latest.get("sparkUser", ""),
        "Started": _ts(latest.get("startTimeEpoch")),
        "Ended": _ts(latest.get("endTimeEpoch")),
    }

    # ── Resources ──
    driver_mem = sp.get("spark.driver.memory", "N/A")
    driver_cores = sp.get("spark.driver.cores", "N/A")
    exec_mem = sp.get("spark.executor.memory", "N/A")
    exec_cores = sp.get("spark.executor.cores", "N/A")
    exec_instances = sp.get("spark.executor.instances", "N/A")
    active_execs = sum(1 for e in executors if e.get("isActive"))
    total_execs = len(executors)
    dyn_alloc = sp.get("spark.dynamicAllocation.enabled", "false")

    resources = {
        "Driver": f"{driver_mem} / {driver_cores} cores",
        "Executors": f"{exec_instances} × {exec_mem} / {exec_cores} cores ({total_execs} total, {active_execs} active)",
        "Dynamic Allocation": dyn_alloc,
        "Shuffle Partitions": sp.get("spark.sql.shuffle.partitions", "200"),
        "Serializer": sp.get("spark.serializer", "JavaSerializer").rsplit(".", 1)[-1],
    }

    # ── Workload ──
    job_statuses = Counter(j.get("status", "UNKNOWN") for j in jobs)
    stage_statuses = Counter(s.get("status", "UNKNOWN") for s in stages)
    sql_statuses = Counter(s.get("status", "UNKNOWN") for s in sqls)

    total_tasks = sum(j.get("numTasks", 0) for j in jobs)
    completed_tasks = sum(j.get("numCompletedTasks", 0) for j in jobs)

    def _status_summary(counts: Counter) -> str:
        total = sum(counts.values())
        parts = []
        for s in ["SUCCEEDED", "COMPLETED", "COMPLETE", "RUNNING", "FAILED", "SKIPPED", "KILLED", "PENDING", "UNKNOWN"]:
            if counts.get(s):
                parts.append(f"{counts[s]} {s.lower()}")
        return f"{total} ({', '.join(parts)})" if parts else str(total)

    workload = {
        "Jobs": _status_summary(job_statuses),
        "Stages": _status_summary(stage_statuses),
        "Tasks": f"{completed_tasks:,}/{total_tasks:,} completed",
        "SQL Executions": _status_summary(sql_statuses),
    }

    return {
        "Application": application,
        "Resources": resources,
        "Workload": workload,
    }


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


def format_task_summary(summary: dict) -> tuple[list[str], list[list[str]]]:
    """Format task summary (quantile distribution) as a table."""
    quantiles = summary.get("quantiles", [])
    headers = ["Metric"] + [f"p{int(q * 100)}" for q in quantiles]

    def _row(label: str, values: list, fmt_fn=None):
        if fmt_fn:
            return [label] + [fmt_fn(v) for v in values]
        return [label] + [f"{v:.0f}" for v in values]

    rows = [
        _row("Duration (ms)", summary.get("duration", [])),
        _row("Executor Run Time (ms)", summary.get("executorRunTime", [])),
        _row("JVM GC Time (ms)", summary.get("jvmGcTime", [])),
        _row("Scheduler Delay (ms)", summary.get("schedulerDelay", [])),
        _row("Deser Time (ms)", summary.get("executorDeserializeTime", [])),
        _row("Result Size", summary.get("resultSize", []), _bytes),
        _row("Peak Memory", summary.get("peakExecutionMemory", []), _bytes),
        _row("Memory Spilled", summary.get("memoryBytesSpilled", []), _bytes),
        _row("Disk Spilled", summary.get("diskBytesSpilled", []), _bytes),
    ]
    # Input metrics
    inp = summary.get("inputMetrics", {})
    if any(v > 0 for v in inp.get("bytesRead", [])):
        rows.append(_row("Input Bytes", inp.get("bytesRead", []), _bytes))
        rows.append(_row("Input Records", inp.get("recordsRead", [])))
    # Output metrics
    out = summary.get("outputMetrics", {})
    if any(v > 0 for v in out.get("bytesWritten", [])):
        rows.append(_row("Output Bytes", out.get("bytesWritten", []), _bytes))
        rows.append(_row("Output Records", out.get("recordsWritten", [])))
    # Shuffle read
    sr = summary.get("shuffleReadMetrics", {})
    if any(v > 0 for v in sr.get("readBytes", sr.get("localBytesRead", []))):
        total_read = sr.get("readBytes", [v1 + v2 for v1, v2 in zip(sr.get("localBytesRead", []), sr.get("remoteBytesRead", []))] if sr.get("localBytesRead") else [])
        if total_read:
            rows.append(_row("Shuffle Read", total_read, _bytes))
        rows.append(_row("Shuffle Read Records", sr.get("readRecords", sr.get("recordsRead", []))))
    # Shuffle write
    sw = summary.get("shuffleWriteMetrics", {})
    if any(v > 0 for v in sw.get("writeBytes", sw.get("bytesWritten", []))):
        rows.append(_row("Shuffle Write", sw.get("writeBytes", sw.get("bytesWritten", [])), _bytes))
        rows.append(_row("Shuffle Write Records", sw.get("writeRecords", sw.get("recordsWritten", []))))

    return headers, rows


def format_task_list(tasks: list[dict]) -> tuple[list[str], list[list[str]]]:
    """Format task list as table headers and rows."""
    headers = ["Task ID", "Index", "Attempt", "Status", "Executor", "Host", "Duration", "GC", "Input", "Shuffle R/W"]
    rows = []
    for t in tasks:
        m = t.get("taskMetrics", {})
        sr = m.get("shuffleReadMetrics", {})
        sw = m.get("shuffleWriteMetrics", {})
        shuffle_read = sr.get("localBytesRead", 0) + sr.get("remoteBytesRead", 0)
        shuffle_write = sw.get("bytesWritten", 0)
        rows.append([
            str(t.get("taskId", "")),
            str(t.get("index", "")),
            str(t.get("attempt", 0)),
            t.get("status", ""),
            t.get("executorId", ""),
            (t.get("host") or "")[:20],
            _duration(t.get("duration")),
            _duration(m.get("jvmGcTime")),
            _bytes(m.get("inputMetrics", {}).get("bytesRead")),
            f"{_bytes(shuffle_read)}/{_bytes(shuffle_write)}",
        ])
    return headers, rows


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
