# Spark Diagnostic Rules

Comprehensive rules for diagnosing Spark application performance issues.
Apply these after collecting data via `spark-history-cli --json`.

## Stage-Level Diagnostics

### Task Skew
**Detection**: Compare p50 and p95 from `stage-summary`:
- p95/p50 > 3x → moderate skew
- p95/p50 > 10x → severe skew
- Also check: max task duration vs median

**Root causes**:
- Uneven partition sizes (data skew)
- Skewed join keys
- Non-splittable file formats or large files

**Recommendations**:
- Enable AQE skew join: `spark.sql.adaptive.skewJoin.enabled=true`
- Increase shuffle partitions to spread data more evenly
- For persistent skew: salting join keys, pre-aggregation
- Check if `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` is appropriate

### GC Pressure
**Detection**: From `executors --all` JSON:
- `totalGCTime / totalDuration > 0.10` → GC concern
- `totalGCTime / totalDuration > 0.20` → severe GC pressure

Per-stage: from `stage-summary`, check `jvmGcTime` quantiles.

**Root causes**:
- Executor memory too small for workload
- Too many tasks per executor (high concurrency)
- Large objects in memory (broadcast variables, cached RDDs)

**Recommendations**:
- Increase `spark.executor.memory`
- Reduce `spark.executor.cores` (fewer concurrent tasks = less memory pressure)
- Increase off-heap memory for Gluten: check native memory settings
- Consider G1GC tuning: `-XX:G1HeapRegionSize`, `-XX:InitiatingHeapOccupancyPercent`

### Shuffle Overhead
**Detection**: From stage detail:
- `shuffleReadBytes + shuffleWriteBytes` > 2x `inputBytes` → shuffle-heavy
- Large `fetchWaitTime` in task metrics → shuffle bottleneck

**Root causes**:
- Too many or unnecessary shuffles (check plan for redundant exchanges)
- Wrong join strategy (SortMergeJoin when BroadcastHashJoin would work)
- Insufficient shuffle partitions causing large partitions

**Recommendations**:
- Enable AQE: `spark.sql.adaptive.enabled=true`
- Auto-coalesce: `spark.sql.adaptive.coalescePartitions.enabled=true`
- Increase broadcast threshold: `spark.sql.autoBroadcastJoinThreshold` (default 10MB)
- For Gluten: prefer `ShuffledHashJoinExecTransformer` over `SortMergeJoin`
- Consider columnar shuffle optimization settings

### Memory Spill
**Detection**: From `stage-summary` or `stage <id>`:
- `memoryBytesSpilled > 0` → memory pressure causing spill
- `diskBytesSpilled > 0` → severe, data spilling to disk

**Root causes**:
- Executor memory insufficient for aggregation/sort buffers
- Too many concurrent tasks sharing memory
- Large shuffle partitions

**Recommendations**:
- Increase `spark.executor.memory`
- Increase shuffle partitions (smaller partitions = less memory per task)
- Tune `spark.memory.fraction` and `spark.memory.storageFraction`
- Reduce `spark.executor.cores` to lower per-executor concurrency

### Straggler Tasks
**Detection**: From `stage-tasks --sort-by -runtime`:
- Top task duration > 5x median → straggler
- Check if stragglers are on specific executors (hardware issue)
- Check if stragglers have much more input (data skew)

**Root causes**:
- Data skew (check input bytes for the slow task)
- Slow executor/node (check if multiple stragglers on same host)
- Speculative execution not enabled
- GC pauses on specific executors

**Recommendations**:
- Enable speculation: `spark.speculation=true`
- Check for data skew (see Task Skew section)
- If host-specific: check for noisy neighbors or hardware issues

## Configuration Diagnostics

### Shuffle Partitions
**Detection**: From `env`:
- `spark.sql.shuffle.partitions` relative to data size
- Rule of thumb: ~128MB per partition for large datasets

**Signs of wrong value**:
- Too few: large partitions, spill, long task durations, OOM
- Too many: overhead from small partitions, scheduler delay dominates

**Recommendations**:
- Enable AQE coalescing: `spark.sql.adaptive.coalescePartitions.enabled=true`
- Set initial value high and let AQE coalesce down
- For TPC-DS SF10000: typically 200-1024 partitions work well

### Executor Sizing
**Detection**: From `summary` and `env`:
- Memory per core = `spark.executor.memory` / `spark.executor.cores`
- < 4GB per core → likely memory pressure
- > 16GB per core → potentially wasting memory

**Recommendations**:
- Sweet spot: 4-8GB per core for most workloads
- For Gluten/Velox: account for off-heap native memory separately
- Total cluster memory = executors × executor-memory; should fit data + overhead

### Serializer
**Detection**: From `env`:
- `spark.serializer` = `JavaSerializer` → suboptimal

**Recommendation**:
- Use `org.apache.spark.serializer.KryoSerializer`
- Register common classes: `spark.kryo.classesToRegister`

### Dynamic Allocation
**Detection**: From `env`:
- `spark.dynamicAllocation.enabled`
- Check if executors were underutilized (many idle executors in `executors --all`)

## AQE Effectiveness

### Detection
Compare initial vs final plan using `sql-plan`:
```bash
spark-history-cli -a <app> sql-plan <exec-id> --view initial
spark-history-cli -a <app> sql-plan <exec-id> --view final
```

**Signs AQE is helping**:
- `SortMergeJoin` in initial → `BroadcastHashJoin` in final (join strategy change)
- Fewer exchange nodes in final plan (partition coalescing)
- `AQEShuffleRead` nodes present (adaptive shuffle)

**Signs AQE could help more**:
- `spark.sql.adaptive.enabled=false` → enable it
- Large `SortMergeJoin` still present when one side is small → lower broadcast threshold
- Many small partitions not coalesced → enable `coalescePartitions`

## Gluten/Velox Diagnostics

### Offloading Coverage
**Detection**: In `sql-plan --view final`:
- Count `*Transformer` / `*ExecTransformer` nodes vs total nodes
- High ratio = good Gluten coverage
- `VeloxColumnarToRow` indicates transition back to Spark (fallback boundary)

### Common Fallback Patterns
- `SortMergeJoin` instead of `ShuffledHashJoinExecTransformer` → join type not supported natively
- `HashAggregate` instead of `RegularHashAggregateExecTransformer` → aggregation fallback
- `Sort` instead of `SortExecTransformer` → sort fallback

### Fallback Impact
When a fallback occurs, data must be converted between columnar (Velox) and row (Spark) format:
- `VeloxColumnarToRow` → native to Spark conversion
- `RowToVeloxColumnar` → Spark to native conversion
- These conversions add overhead; minimize them by ensuring contiguous native execution

## Thresholds Summary

| Metric | OK | Warning | Critical |
|--------|-----|---------|----------|
| GC/Duration ratio | < 5% | 5-15% | > 15% |
| Task skew (p95/p50) | < 2x | 2-5x | > 5x |
| Memory spill | 0 | < 10% of input | > 10% of input |
| Disk spill | 0 | Any | Large |
| Shuffle/Input ratio | < 1x | 1-3x | > 3x |
| Partition size | 64-256MB | 32-512MB | < 16MB or > 1GB |
| Memory per core | 4-8GB | 2-4GB or 8-16GB | < 2GB or > 16GB |
