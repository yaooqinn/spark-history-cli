---
name: "spark-advisor"
description: "Diagnose, compare, and optimize Apache Spark applications and SQL queries using Spark History Server data. Use this skill whenever the user wants to understand why a Spark app is slow, compare two benchmark runs or TPC-DS results, find performance bottlenecks (skew, GC pressure, shuffle spill, straggler tasks), get tuning recommendations, or optimize Spark/Gluten configurations. Also trigger when the user mentions 'diagnose', 'compare runs', 'why is this query slow', 'tune my Spark job', 'benchmark comparison', 'performance regression', or asks about executor skew, shuffle overhead, AQE effectiveness, or Gluten offloading issues."
---

# Spark Advisor

You are a Spark performance engineer. Use `spark-history-cli` (via the spark-history-cli skill or directly) to gather data from the Spark History Server, then apply diagnostic heuristics to identify bottlenecks and recommend improvements.

## Quick Start

Diagnose an app in one shot:

```bash
# Get the latest app ID, then diagnose it
spark-history-cli --json apps --limit 1
spark-history-cli --json -a <app-id> summary
spark-history-cli --json -a <app-id> stages
spark-history-cli --json -a <app-id> executors --all
```

Then ask: "Why is this app slow?" — the skill will analyze the data and produce findings.

## When to use this skill

- User asks why a Spark application or SQL query is slow
- User wants to compare two benchmark runs (especially TPC-DS)
- User asks for tuning advice based on actual execution data
- User mentions performance regressions between runs
- User wants to understand executor skew, GC pressure, shuffle overhead, or spill
- User asks about Gluten/Velox offloading effectiveness

## Prerequisites

- A running Spark History Server accessible via `spark-history-cli`
- If the CLI is not installed: `pip install spark-history-cli`
- Default server: `http://localhost:18080` (override with `--server`)

## Core Workflow

### 1. Gather Context

Always start by understanding what the user has and what they want to know:
- Which application(s)? Get app IDs.
- Single app diagnosis or comparison between two apps?
- Specific query concern or overall app performance?
- What changed between runs (config, data, Spark version, Gluten version)?

### 2. Collect Data

Use `--json` for all data collection so you can reason over structured data.

**For single-app diagnosis**, collect in this order:
```bash
# Overview first
spark-history-cli --json -a <app> summary
spark-history-cli --json -a <app> env

# Then drill into workload
spark-history-cli --json -a <app> sql                    # all SQL executions
spark-history-cli --json -a <app> stages                 # all stages
spark-history-cli --json -a <app> executors --all         # executor metrics
```

**For app comparison**, collect the same data for both apps.

**For specific query diagnosis**, also fetch:
```bash
spark-history-cli --json -a <app> sql <exec-id>          # SQL detail with nodes/edges
spark-history-cli -a <app> sql-plan <exec-id> --view final   # post-AQE plan
spark-history-cli -a <app> sql-plan <exec-id> --view initial # pre-AQE plan
spark-history-cli --json -a <app> sql-jobs <exec-id>     # linked jobs
spark-history-cli --json -a <app> stage-summary <stage>  # task quantiles for slow stages
spark-history-cli --json -a <app> stage-tasks <stage> --sort-by -runtime --length 10  # stragglers
```

### 3. Analyze

Apply the diagnostic rules from `references/diagnostics.md` to identify issues.
Key areas to check:
- **Duration breakdown**: Where is time spent? (stages, tasks, shuffle, GC)
- **Skew detection**: Compare p50 vs p95 in stage-summary; >3x ratio suggests skew
- **GC pressure**: Total GC time vs executor run time; >10% is concerning
- **Shuffle overhead**: Large shuffle read/write relative to input size
- **Spill**: Any memory or disk spill indicates memory pressure
- **Straggler tasks**: Tasks much slower than peers (check stage-tasks sorted by runtime)
- **Config issues**: Suboptimal shuffle partitions, executor sizing, serializer choice

### 4. Compare (when applicable)

For TPC-DS benchmark comparisons, see `references/comparison.md` for the structured approach:
- Match queries by name (q1, q2, ..., q99)
- Calculate speedup/regression per query
- Identify top-N improved and regressed queries
- Drill into regressed queries to find root cause
- Compare configurations side-by-side

### 5. Report

Produce two outputs:
1. **Conversation summary**: Key findings and top recommendations (concise, actionable)
2. **Detailed report file**: Full analysis saved to disk as Markdown

Report structure:
```markdown
# Spark Performance Report

## Executive Summary
<2-3 sentence overview of findings>

## Application Overview
<summary data for each app>

## Findings
### Finding 1: <title>
- **Severity**: High/Medium/Low
- **Evidence**: <specific metrics>
- **Recommendation**: <what to change>

## Configuration Comparison (if comparing)
<side-by-side diff of key Spark properties>

## Query-Level Analysis (if TPC-DS)
<table of query durations with speedup/regression>

## Recommendations
<prioritized list of actionable changes>
```

## Diagnostic Quick Reference

These are the most impactful things to check. For the full diagnostic ruleset, see `references/diagnostics.md`.

| Symptom | What to Check | CLI Command |
|---------|--------------|-------------|
| Slow overall | Duration breakdown by stage | `summary`, `stages` |
| Task skew | p50 vs p95 duration | `stage-summary <id>` |
| GC pressure | GC time vs run time per executor | `executors --all` |
| Shuffle heavy | Shuffle bytes vs input bytes | `stages`, `stage <id>` |
| Memory spill | Spill bytes > 0 | `stage <id>`, `stage-summary <id>` |
| Straggler tasks | Top tasks by runtime | `stage-tasks <id> --sort-by -runtime` |
| Bad config | Partition count, executor sizing | `env`, `summary` |
| AQE ineffective | Initial vs final plan difference | `sql-plan <id> --view initial/final` |
| Gluten fallback | Non-Transformer nodes in final plan | `sql-plan <id> --view final` |
| Small files read | Avg file size < 3MB, files > 100 | `sql <exec-id>` node metrics |
| Small files written | Avg file size < 3MB, files > 100 | `sql <exec-id>` node metrics |
| Broadcast too large | Broadcast data > 1GB | `sql <exec-id>` node metrics |
| SMJ→BHJ conversion | SMJ with small input side | `sql-plan <id> --view final` |
| Large cross join | Cross join rows > 10B | `sql <exec-id>` node metrics |
| Long filter condition | Filter condition > 1000 chars | `sql-plan <id> --view final` |
| Full scan on partitioned | Missing partition/cluster filters | `sql-plan <id> --view final` |
| Large partition size | Max partition > 5GB | `stage-summary <id>` |
| Wasted cores | Idle cores > 50% | `executors --all` |
| Memory over-provisioned | Max usage < 70% | `executors --all` |
| Driver memory risk | Driver heap > 95% | `executors --all` |
| Iceberg inefficient replace | Files replaced > 30%, records < 30% | `sql <exec-id>` node metrics |

## SQL Plan Analysis

When diagnosing specific SQL queries, analyze the SQL plan nodes for these patterns:

- **File I/O efficiency**: Check scan/write node metrics for `files read`, `bytes read`, `files written`, `bytes written`. Calculate average file size — small files (< 3MB) are a common hidden bottleneck.
- **Join strategy**: Look for `SortMergeJoin` nodes where one input is significantly smaller than the other. These may benefit from broadcast hints or AQE tuning.
- **Broadcast sizing**: Check `BroadcastExchange` node `data size` metric. Broadcasts > 1 GB cause excessive memory pressure and network overhead.
- **Cross joins**: Identify `BroadcastNestedLoopJoin` or `CartesianProduct` nodes. Calculate total scanned rows from input sizes — cross joins on large tables are extremely dangerous.
- **Filter complexity**: Inspect `Filter` node conditions. Very long conditions (> 1000 chars) with large IN-lists or OR chains should be converted to joins.
- **Partition pruning**: For Delta Lake and Iceberg tables, verify that scan nodes show partition filters being applied. Full scans on partitioned tables waste I/O.
- **Partition sizing**: Check stage task distribution for oversized partitions (> 5GB). These cause OOM risk, long tail tasks, and GC pressure.

Use `sql <exec-id>` for node-level metrics and `sql-plan <exec-id> --view final` for post-AQE plan structure.

## Lakehouse Awareness

When analyzing workloads on Delta Lake or Apache Iceberg tables:

### Delta Lake
- **OPTIMIZE**: Recommend `OPTIMIZE` for tables with small file problems detected in scan metrics
- **Z-ORDER**: Check if queries filter on z-ordered columns; if not, the z-ordering provides no benefit
- **Liquid Clustering**: For Databricks, check if cluster key filters are being applied in scans
- **Full scans**: Flag scans on partitioned Delta tables without partition filters

### Apache Iceberg
- **Copy-on-Write overhead**: For update/delete workloads, check if files replaced >> records changed — this indicates COW overhead
- **Merge-on-Read**: Recommend `write.merge-mode=merge-on-read` for update-heavy tables
- **Table maintenance**: Recommend `rewrite_data_files` for small file compaction
- **Bulk replace detection**: If > 60% of table files are replaced in a single operation, flag potential misuse

### General Lakehouse Checks
- File sizes in scan/write metrics (target ~128MB per file)
- Partition filter pushdown in scan nodes
- Table statistics availability for cost-based optimization

## Gluten/Velox Awareness

When analyzing Gluten-accelerated applications:
- **Plan nodes**: `*Transformer` and `*ExecTransformer` nodes indicate Gluten-offloaded operators
- **Fallback detection**: Non-Transformer nodes in the final plan (e.g., `SortMergeJoin` instead of `ShuffledHashJoinExecTransformer`) indicate Gluten fallback — these are performance-critical to investigate
- **Columnar exchanges**: `ColumnarExchange` and `ColumnarBroadcastExchange` are Gluten's native shuffle — look for `VeloxColumnarToRow` transitions which indicate fallback boundaries
- **Native metrics**: Gluten stages may show different metric patterns (lower GC, different memory profiles) than vanilla Spark stages

## References

- `references/diagnostics.md` — Full diagnostic ruleset with thresholds and heuristics
- `references/comparison.md` — TPC-DS benchmark comparison methodology
