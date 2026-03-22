# Spark Diagnostic Rules

Comprehensive rules for diagnosing Spark application performance issues.
Apply these after collecting data via `spark-history-cli --json`.

## Data Availability

All diagnostics in this file use data from the **standard Spark History Server REST API** (`/api/v1/`). No additional plugins or instrumentation are required — works with vanilla OSS Apache Spark.

**Note**: The Lakehouse-Specific Diagnostics section (Iceberg/Delta Lake) requires metadata that is only available when those frameworks expose metrics through Spark's SQL plan nodes. If the data is not present, those rules simply won't trigger.

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

## SQL-Level Diagnostics

### Small Files Read
**Detection**: From SQL plan node metrics (`files read` and `bytes read`):
- Average file size < 3 MB AND files read > 100 → small files problem

**Root causes**:
- Data written with too many partitions
- High-cardinality partition keys
- Frequent small-batch writes

**Recommendations**:
- Ask data owner to compact/repartition source data
- Reduce executors to amortize small file overhead
- Use table maintenance (OPTIMIZE for Delta, rewrite_data_files for Iceberg)

### Small Files Written
**Detection**: From SQL plan node metrics (`files written` and `bytes written`):
- Average file size < 3 MB AND files written > 100 → writing small files
- Ideal target: ~128 MB per file
- For partitioned writes: check files per partition

**Root causes**:
- Too many output partitions
- High-cardinality partition keys

**Recommendations**:
- For unpartitioned: `.repartition(N)` before write where N = total_bytes / 128MB
- For partitioned: `.repartition("partition_key")` before write
- Choose partition keys with lower cardinality

### Broadcast Too Large
**Detection**: From SQL plan BroadcastExchange node metrics (`data size`):
- Broadcast data size > 1 GB → too large for broadcast

**Root causes**:
- `spark.sql.autoBroadcastJoinThreshold` set too high
- Broadcast hint on large table

**Recommendations**:
- Lower `spark.sql.autoBroadcastJoinThreshold`
- Remove broadcast hint from large DataFrames
- Consider SortMergeJoin for tables > 1 GB

### SortMergeJoin Should Be BroadcastHashJoin
**Detection**: From SQL plan - SortMergeJoin node where one input is much smaller:
- Small table < 10 MB (AQE should have caught this)
- Small table < 100 MB AND large table > 10 GB
- Small table < 1 GB AND large table > 300 GB
- Small table < 5 GB AND large table > 1 TB

**Root causes**:
- AQE disabled or couldn't estimate sizes
- Missing statistics

**Recommendations**:
- Use `broadcast(small_df)` hint
- Increase `spark.sql.autoBroadcastJoinThreshold`
- Ensure AQE is enabled: `spark.sql.adaptive.enabled=true`

### Large Cross Join
**Detection**: From BroadcastNestedLoopJoin or CartesianProduct node metrics:
- Cross Join Scanned Rows > 10 billion → dangerous cross join

**Root causes**:
- Missing join conditions
- Accidental Cartesian product

**Recommendations**:
- Add specific join conditions
- Avoid cross joins on large datasets
- Consider alternatives (window functions, explode + join)

### Long Filter Conditions
**Detection**: From Filter node plan condition:
- Condition string length > 1000 characters → performance risk

**Root causes**:
- Large IN-lists
- Complex OR chains
- Programmatically generated filters

**Recommendations**:
- Convert filter to a join (create DataFrame of filter values, inner join)
- Rewrite filter to be shorter
- Use temp table for large value lists

### Full Scan on Partitioned/Clustered Tables
**Detection**: From scan nodes with Delta Lake / Iceberg metadata:
- Partitioned table scanned without partition filters
- Liquid Clustering table scanned without cluster key filters
- Z-Ordered table scanned without z-order column filters

**Root causes**:
- Missing WHERE clauses on partition/cluster keys

**Recommendations**:
- Add filter on partition key(s)
- Add filter on clustering key(s)
- Review query to ensure predicate pushdown works

### Large Partition Size
**Detection**: From stage task distribution metrics:
- Max partition size > 5 GB (input, output, shuffle read, or shuffle write)

**Root causes**:
- Uneven data distribution
- Too few partitions

**Recommendations**:
- Increase number of partitions
- Use more specific partitioning keys
- Enable AQE auto-coalesce

## Resource Utilization Diagnostics

### Wasted Cores / Over-Provisioned Cluster
**Detection**: From executor metrics:
- Idle cores rate > 50% → cluster over-provisioned

**Root causes**:
- Too many executors/cores for the workload size

**Recommendations**:
- For static allocation: lower `spark.executor.cores` or `spark.executor.instances`
- For dynamic allocation: tune `spark.dynamicAllocation.executorAllocationRatio` or increase `spark.dynamicAllocation.schedulerBacklogTimeout`

### Executor Memory Over-Provisioned
**Detection**: From executor memory metrics:
- Max executor memory usage < 70% → over-provisioned (wasting money)
- Max executor memory usage > 95% → under-provisioned (risk of OOM/spill)

**Root causes**:
- Wrong `spark.executor.memory` sizing

**Recommendations**:
- Over-provisioned: decrease `spark.executor.memory` to max_usage * 1.2
- Under-provisioned: increase `spark.executor.memory` by 20%

### Driver Memory Under-Provisioned
**Detection**: From driver executor heap memory usage:
- Driver heap usage > 95% of Xmx → risk of driver OOM

**Root causes**:
- Large collect() calls
- Too many broadcast variables
- Driver-side aggregations

**Recommendations**:
- Increase `spark.driver.memory`
- Avoid `collect()` on large datasets
- Reduce broadcast variable sizes

## Lakehouse-Specific Diagnostics

### Inefficient Iceberg Table Replace
**Detection**: From Iceberg commit metrics on ReplaceData operations:
- Table files replaced > 30% BUT records changed < 30% → rewriting too many files

**Root causes**:
- Copy-on-write mode rewriting entire files for small updates

**Recommendations**:
- Switch to merge-on-read mode (`write.merge-mode=merge-on-read`)
- Partition table so updates touch fewer partitions

### Replaced Most of Iceberg Table
**Detection**: From Iceberg commit metrics:
- Table files replaced > 60% → potential misuse of Iceberg

**Root causes**:
- Bulk updates/deletes that rewrite most of the table

**Recommendations**:
- Partition table to localize updates
- Consider if Iceberg is the right format for this workload

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
| Avg file size (read/write) | > 64MB | 3-64MB | < 3MB |
| Broadcast data size | < 256MB | 256MB-1GB | > 1GB |
| Cross join rows | < 1B | 1-10B | > 10B |
| Filter condition length | < 500 chars | 500-1000 chars | > 1000 chars |
| Max partition size | < 2GB | 2-5GB | > 5GB |
| Idle cores rate | < 20% | 20-50% | > 50% |
| Executor memory usage | 70-90% | 50-70% or 90-95% | < 50% or > 95% |
| Driver heap usage | < 80% | 80-95% | > 95% |
| Iceberg files replaced | < 30% | 30-60% | > 60% |
