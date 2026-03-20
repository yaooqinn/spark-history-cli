# TPC-DS Benchmark Comparison

Methodology for comparing two TPC-DS benchmark runs using `spark-history-cli`.

## Step 1: Identify the Two Runs

Ask the user for two application IDs, or find them automatically:
```bash
spark-history-cli --json apps --status completed --limit 10
```

Label them as **baseline** (older/reference run) and **candidate** (newer/test run).

## Step 2: Collect Summaries

For each app:
```bash
spark-history-cli --json -a <app> summary
spark-history-cli --json -a <app> env
spark-history-cli --json -a <app> sql
```

## Step 3: Match Queries

TPC-DS queries are identified by their SQL execution `description` field, which typically contains the query name (e.g., "Query - q1", "q1 [i:1]").

Parse the description to extract query names and match them across the two runs.

**Matching rules**:
- Strip prefixes like "Query - ", "Delta: Query - ", etc.
- Match on the base query name (q1, q2, ..., q99, q14a, q14b, q23a, q23b, q24a, q24b)
- TPC-DS has 99 queries but some are split (q14a/b, q23a/b, q24a/b, q39a/b) = ~103 total
- If a query ran multiple iterations, use the first successful run or the fastest

## Step 4: Calculate Metrics

For each matched query pair:
```
duration_baseline = baseline SQL execution duration (ms)
duration_candidate = candidate SQL execution duration (ms)
speedup = duration_baseline / duration_candidate
regression = duration_candidate / duration_baseline (if > 1.0)
delta_seconds = (duration_candidate - duration_baseline) / 1000
```

Aggregate metrics:
```
total_baseline = sum of all baseline query durations
total_candidate = sum of all candidate query durations
overall_speedup = total_baseline / total_candidate
geomean_speedup = geometric mean of per-query speedups
```

## Step 5: Produce Comparison Table

Sort queries by absolute time delta (largest regression first):

```markdown
| Query | Baseline | Candidate | Delta | Speedup | Status |
|-------|----------|-----------|-------|---------|--------|
| q67   | 72s      | 85s       | +13s  | 0.85x   | ⚠ REGRESSED |
| q1    | 61s      | 45s       | -16s  | 1.36x   | ✓ IMPROVED |
| ...   | ...      | ...       | ...   | ...     | ... |
```

**Status labels**:
- `✓ IMPROVED`: speedup > 1.05x (>5% faster)
- `≈ NEUTRAL`: speedup between 0.95x and 1.05x
- `⚠ REGRESSED`: speedup < 0.95x (>5% slower)

## Step 6: Drill into Regressions

For the top-3 regressed queries, investigate root cause:

1. **Compare plans**: Fetch `sql-plan --view final` for both apps
   - Did the plan change? (different join strategies, missing Gluten offloading)
   - Did AQE make different decisions?

2. **Compare stage metrics**: For the slowest stages in each
   - Check task skew (`stage-summary`)
   - Check shuffle size changes
   - Check GC time differences

3. **Compare configurations**: Diff the `env` output
   - Focus on: shuffle partitions, memory, broadcast threshold, AQE settings
   - For Gluten: check native engine version, offload settings

## Step 7: Configuration Diff

Extract key Spark properties from both apps' `env` and show differences:

```markdown
| Property | Baseline | Candidate |
|----------|----------|-----------|
| spark.executor.memory | 56g | 64g |
| spark.sql.shuffle.partitions | 200 | 512 |
| spark.sql.adaptive.enabled | true | true |
```

Focus on properties that differ and are performance-relevant:
- `spark.executor.memory`, `spark.executor.cores`, `spark.executor.instances`
- `spark.sql.shuffle.partitions`
- `spark.sql.adaptive.*`
- `spark.sql.autoBroadcastJoinThreshold`
- `spark.serializer`
- Any `spark.gluten.*` or `spark.plugins` changes

## Step 8: Recommendations

Based on the comparison findings, prioritize recommendations:

1. **If overall regression**: Focus on the top regressed queries and their root causes
2. **If overall improvement but some regressions**: Note the wins, investigate the regressions
3. **If config change caused issues**: Recommend reverting specific settings
4. **If plan changes caused regression**: Recommend query hints or optimizer settings

## Report Template

```markdown
# TPC-DS Benchmark Comparison

## Overview
- **Baseline**: <app-name> (<app-id>) — <duration>
- **Candidate**: <app-name> (<app-id>) — <duration>
- **Overall**: <speedup>x (total <baseline-total>s → <candidate-total>s)
- **Improved**: N queries | **Regressed**: N queries | **Neutral**: N queries

## Configuration Changes
<config diff table>

## Query Results
<full comparison table sorted by delta>

## Top Regressions
### q67: +13s (0.85x)
- **Root cause**: <explanation>
- **Evidence**: <specific metrics>

## Top Improvements
### q1: -16s (1.36x)
- **Likely cause**: <explanation>

## Recommendations
1. <highest impact recommendation>
2. <second recommendation>
3. ...
```
