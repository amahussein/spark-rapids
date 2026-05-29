---
layout: page
title: CuBF GPU Exec Cost & Sizing Analysis
nav_order: 19
parent: Developer Overview
---
# CuBF GPU Exec Cost & Sizing Analysis

Companion to [CuBF GPU Exec Design](rapids_exec_cubf_design.md). The primary design doc's appendix
(§8.3 Cost & Resource Profile, §8.4 Limitations and Resource Risks) points here for the full
treatment. This document holds the per-task and total overhead derivations, the sensitivity tables,
the worked sizing example, the cost-telemetry caveats, and the quantitative resource-risk and
configuration-derivation detail. It is observability and capacity-planning analysis only — every
correctness invariant lives in the primary design doc, and the sections below cross-reference it
rather than restate it.

This document covers the following topics:

* [1. Cost Model Overview](#1-cost-model-overview)
* [2. Build-Side Overhead Per Task](#2-build-side-overhead-per-task)
* [3. Build-Side Overhead Total](#3-build-side-overhead-total)
* [4. Probe-Side Overhead Per Task](#4-probe-side-overhead-per-task)
* [5. What Drives Total Overhead](#5-what-drives-total-overhead)
* [6. Sensitivity Analysis](#6-sensitivity-analysis)
* [7. Worked Sizing Example](#7-worked-sizing-example)
* [8. Cost Telemetry Under Partial Drain](#8-cost-telemetry-under-partial-drain)
* [9. Limitations and Resource Risks (Quantitative Detail)](#9-limitations-and-resource-risks-quantitative-detail)
  * [9.1 GPU Memory on Executors](#91-gpu-memory-on-executors)
  * [9.2 Network Between Executors and Driver](#92-network-between-executors-and-driver)
  * [9.3 Driver Heap and Merge CPU](#93-driver-heap-and-merge-cpu)
  * [9.4 Diagnostic Accumulator Cache Lifecycle](#94-diagnostic-accumulator-cache-lifecycle)
* [10. Configuration Derivation Summary](#10-configuration-derivation-summary)

## 1. Cost Model Overview

CuBF adds overhead in three locations: GPU memory on executors during build and probe, network
between executors and the driver during accumulator shipping, and driver JVM heap for merged bloom
filter bytes and merge CPU. The build and probe phases do not overlap in time: the build stage must
complete and deliver merged bytes before any probe task can consume them, so build-side and
probe-side GPU costs are never concurrent for the same bloom filter.

Per-bloom-filter size is the shared scaling factor used throughout this document:

```
bf_bytes = ceil(numBits / 8) + header_bytes

header_bytes = 12   (V1, Spark 3.x)
             = 16   (V2, Spark 4.x)
```

`numBits` is determined by the planner heuristic. Typical sizing uses `numItems * bitsPerItem`,
where `bitsPerItem` depends on the target false-positive probability: about 10 bits/item at FPP 0.01
with 7 hashes, and about 5 bits/item at FPP 0.1 with 3 hashes.

A size guard caps `bf_bytes` at `effectiveMaxFilterBytes`. The cap is resolved from an optional
capability helper and falls back to the approximate 256 MB V1 indexing ceiling. Bloom filters
exceeding the cap are marked skipped before any GPU allocation or network transfer.

## 2. Build-Side Overhead Per Task

Each build-side task processes one partition of the build child stream. For `K` bloom filter specs
in one `GpuGenerateCuBFExec`, the persistent GPU memory during the task lifetime is:

```
GPU_build_persistent = K * bf_bytes
```

These are GPU bloom filter scalars. They are lazily created on the first non-empty batch and held
until finalize. For typical dimension-side bloom filters, from 1 KB to 200 KB, this is negligible.
Near a configured size cap, for example 8 MB per bloom filter, `K=3` would be 24 MB per task.

Transient GPU memory per batch is:

```
GPU_hash_transient = batch_rows * 8 bytes
```

This is one XxHash64 `INT64` column. The build loop processes specs sequentially, so only one hash
column exists at a time. At a typical RAPIDS batch target of about 1M rows, this is about 8 MB. This
cost is constant regardless of bloom filter size; it depends only on batch row count.

Transient host memory at finalize is:

```
Host_finalize = K * 2 * bf_bytes
```

This accounts for one host buffer for the GPU-to-host copy plus one `byte[]` for the accumulator
payload per spec. Both are short-lived: allocated at finalize, then released after the accumulator
update.

GPU kernel launches per batch are:

```
kernels_per_batch = K * 2
```

There is one XxHash64 kernel and one bloom filter `put` kernel per spec. GPU kernel launch overhead
is roughly 5 to 10 microseconds each. For `K=3` and 50 batches per partition, that is 300 launches,
or about 1.5 to 3 ms total, which is negligible relative to batch processing time.

Build wall time reported via build-side `CuBFDiagPairMetric` updates is partition-level (one
measurement per task), shared across all specs in the operator. With K specs per operator, the same
wall-time value appears in K updater calls. Driver-side cost analysis must divide by spec count for
per-BF attribution.

## 3. Build-Side Overhead Total

Network overhead from executors to the driver is:

```
Network_build = P_build * K * bf_bytes
```

`P_build` is the number of build-side partitions. Each task ships `K` serialized byte arrays to the
driver through Spark's accumulator protocol as part of task completion messages, not as separate
transfers. This is the dominant build-side overhead for large bloom filters with many partitions.
See § 9.2 for the relationship between per-task accumulator payload and
`spark.driver.maxResultSize`.

Driver heap for merged bloom filters is:

```
Driver_heap_build = K * bf_bytes
```

The driver holds one merged `byte[]` per `bfId`. Merge is incremental: each partition's bytes are
OR'd into the existing merged array. Peak heap is one copy per bloom filter, not `P_build` copies,
plus a transient second copy during each merge step.

Driver CPU merge work is:

```
Driver_merge_bytes = K * (P_build - 1) * (bf_bytes - header_bytes)
```

This is the total data-section byte count OR'd across all merges. It runs single-threaded on the
DAGScheduler event loop. For typical dimension-side bloom filter sizes (under 200 KB), merge
overhead per partition is negligible.

## 4. Probe-Side Overhead Per Task

Each probe task deserializes the full merged bloom filter once, via a lazy value triggered on first
`columnarEval`, and holds it for the task lifetime.

Persistent probe GPU memory during the task lifetime is:

```
GPU_probe_persistent = S * bf_bytes
```

`S` is the number of stacked bloom filter predicates on the same probe-side scan. If `K` bloom
filters probe different join stages, each task holds only one filter (`S=1`). If all `K` are stacked
on the same probe table, each probe task holds all `K` filters (`S=K`).

Each deserialized bloom filter is wrapped in a `SpillableBuffer` at `ACTIVE_ON_DECK_PRIORITY`. Under
GPU memory pressure, bloom filters can spill to host memory. Re-materialization requires one
host-to-device copy per batch that accesses a spilled bloom filter.

Per-executor probe GPU memory is:

```
GPU_probe_per_executor = T * S * bf_bytes
```

`T` is the effective number of concurrent GPU tasks per executor. It is bounded by both CPU task
slots and GPU resource slots:

```
T = min(executor_cores / spark.task.cpus,
        executor_gpus / spark.task.resource.gpu.amount)
```

`executor_cores` is the number of cores assigned to the executor (`spark.executor.cores`).
`executor_gpus` is the number of GPU resources assigned to the executor
(`spark.executor.resource.gpu.amount`).

Transient GPU memory per probe batch is:

```
GPU_probe_batch = batch_rows * 1 byte
```

This is the `BOOL8` result column. At 1M rows per batch, it is about 1 MB. It is created per
`columnarEval`, consumed by the parent filter, then closed.

When probe instrumentation is enabled, one additional `sum(DType.INT64)` reduction produces a scalar
per batch per stacked bloom filter. This is sub-microsecond on the GPU. The `(rowsIn, rowsPassed)`
tuple piggybacks on Spark's accumulator heartbeat protocol.

## 5. What Drives Total Overhead

| Resource | Dominant factor | Formula | When it matters |
|----------|-----------------|---------|-----------------|
| Executor GPU, build | Specs times bloom filter size | `K * bf_bytes` per task | Near size cap with multiple specs |
| Executor GPU, probe | Tasks times stacked filters times size | `T * S * bf_bytes` per executor | Near size cap with high task concurrency |
| Network | Partitions times specs times size | `P_build * K * bf_bytes` total | Large filters with many build partitions |
| Driver heap | Number of unique filters | `K * bf_bytes` | Near size cap with many concurrent filters |
| Driver CPU | Merge operations | `K * P_build * bf_bytes` bytes OR'd | Many build partitions |

For most queries, bloom filter sizes are 1 KB to 200 KB and all overhead dimensions are negligible
relative to the data volumes being filtered.

## 6. Sensitivity Analysis

The following tables show how overhead scales with the primary tuning parameters. All values assume
V1 format with a 12-byte header. The GPU hash transient cost, about 8 MB per batch at 1M rows, is
constant across all rows and is omitted.

Table A shows overhead versus bloom filter size with `K=1`, `P_build=200`, and `T=16` tasks per
executor. This is the primary sensitivity because the bloom filter size cap is the main
user-controlled knob.

| `bf_bytes` | GPU build/task | Network total | Driver heap | GPU probe/executor |
|------------|----------------|---------------|-------------|--------------------|
| 64 KB | 64 KB | 12.5 MB | 64 KB | 1 MB |
| 256 KB | 256 KB | 50 MB | 256 KB | 4 MB |
| 1 MB | 1 MB | 200 MB | 1 MB | 16 MB |
| 4 MB | 4 MB | 800 MB | 4 MB | 64 MB |
| 8 MB | 8 MB | 1.6 GB | 8 MB | 128 MB |

All dimensions scale linearly with `bf_bytes`. Network is the first dimension to become non-trivial
because it multiplies by `P_build`. At the 8 MB fact cap, network totals 1.6 GB. This is still
modest compared to the multi-TB shuffle volumes these bloom filters target, but it is visible in
Spark UI accumulator panels.

Table B shows probe GPU memory versus bloom filter size and concurrency per executor. Rows vary
`bf_bytes`; columns vary `T * S`, the product of concurrent tasks and stacked bloom filters per
task.

| `bf_bytes` | `T*S=1` | `T*S=4` | `T*S=16` | `T*S=48` |
|------------|---------|---------|----------|----------|
| 65 KB | 65 KB | 260 KB | 1 MB | 3 MB |
| 256 KB | 256 KB | 1 MB | 4 MB | 12 MB |
| 1 MB | 1 MB | 4 MB | 16 MB | 48 MB |
| 4 MB | 4 MB | 16 MB | 64 MB | 192 MB |
| 8 MB | 8 MB | 32 MB | 128 MB | 384 MB |

The `T*S=48` column represents 16 concurrent tasks with 3 stacked bloom filters each. At 8 MB per
bloom filter this reaches 384 MB per executor, which is significant on a 16 to 24 GB GPU. However,
all probe-side bloom filters are wrapped in `SpillableBuffer`, so they are eligible for RAPIDS
memory spilling under GPU pressure. A planner cap that keeps bloom filters in the sub-200-KB range
places the probe-side operating point below 10 MB per executor even at `T*S=48`.

Table C shows network overhead versus build partitions with `K=1`. Network overhead is linear in
both `bf_bytes` and `P_build`.

| `bf_bytes` \\ `P_build` | 1 | 10 | 50 | 200 |
|-------------------------|---|----|----|-----|
| 65 KB | 65 KB | 650 KB | 3.2 MB | 12.7 MB |
| 1 MB | 1 MB | 10 MB | 50 MB | 200 MB |
| 4 MB | 4 MB | 40 MB | 200 MB | 800 MB |
| 8 MB | 8 MB | 80 MB | 400 MB | 1.6 GB |

Small dimension tables, for example `date_dim` with `P_build` near 1, produce negligible network
traffic regardless of bloom filter size. Network overhead becomes material only when large bloom
filters are built over many-partition tables.

## 7. Worked Sizing Example

This example uses representative values to show how the formulas above should be applied. It is not
tied to a particular benchmark, deployment, or validation run.

Assume:

- `K=3` bloom filters are coalesced into one build exec.
- `S=3` bloom filter predicates are stacked on the probe side.
- `T=14` tasks can run concurrently per executor.
- `P_build=200` build partitions contribute partial bloom filters.
- Each serialized bloom filter is about 65 KB including the wire-format header.
- The build key batch has 1M rows when the hash column is materialized.

Build-side per task:

- GPU persistent: `3 * 65 KB = 195 KB`.
- GPU hash transient: `1M rows * 8 = 8 MB`, one spec at a time.
- Host finalize: `3 * 2 * 65 KB = 390 KB`, transient.

Build-side total:

- Network: `200 * 3 * 65 KB = about 38 MB`.
- Driver heap: `3 * 65 KB = 195 KB`.
- Driver merge CPU: `199 * 3 * 65 KB = about 38 MB` OR'd, negligible at this
filter size.

Probe-side per task with three stacked bloom filters:

- GPU persistent: `3 * 65 KB = 195 KB`, each in a `SpillableBuffer`.
- GPU transient: about 1 MB per batch for the `BOOL8` result.

Probe-side per executor:

- GPU persistent: `14 * 3 * 65 KB = about 2.7 MB`.

This example shows why small dimension-style bloom filters have low fixed overhead even when
multiple filters share the same build side. The planner's bloom filter size cap is still the primary
lever for bounding worst-case overhead: filters exceeding the configured cap are skipped before any
GPU allocation or network transfer occurs. The sensitivity tables above show the overhead envelope
for capacity planning when adjusting this cap.

## 8. Cost Telemetry Under Partial Drain

The per-task and total cost figures above assume every build partition drains to completion — each
build-side task consumes its upstream iterator to exhaustion and finalizes its bloom filters. That
assumption does not always hold, and when it is violated the build-cost telemetry under-counts.

When a build partition's upstream iterator is abandoned early — for example a downstream `LIMIT` /
`take(N)` pushdown that stops pulling rows once enough have been produced — the operator's
task-completion listener emits the skip sentinel for that partition's bloom filter and does **not**
record a build-cost update for it.

Because the build-cost diagnostic metric sums per-partition contributions, a short-drained partition
contributes `(build_wall = 0, bf_bytes = 0)`. As a result, aggregate build-cost telemetry slightly
**under-counts** true CPU/build time, and per-partition cost attribution is unreliable for any
partition that short-drained.

This is **observability-only**. Join correctness is unaffected: the skip sentinel poisons the merged
bloom filter (fail-closed — see the delivery contract, closing behavior, and accumulator-merge
behavior in the primary design doc, §5.5 and §7.3–7.5), so the probe side falls back to a no-op
filter and the join output is identical to a run with no bloom filter at all. The cost numbers being
slightly low does not change results.

Treat this as a known, accepted limitation of the diagnostic metrics, not a defect to be fixed. The
cost figures elsewhere in this document assume full drain and should be read as upper-attribution
estimates whenever partial drains occur.

## 9. Limitations and Resource Risks (Quantitative Detail)

This section gives the full quantitative basis for the cuBF operator's resource risks — the
per-dimension formulas, the worst-case numbers, and how to derive safe configuration values from
cluster resources. The primary design doc's appendix (§8.4) points here for them; this is their
authoritative home.

### 9.1 GPU Memory on Executors

**Risk.** GPU memory scales with concurrent tasks per executor. During the build stage, each task
holds `K` bloom filter GPU Scalars, so per-executor build memory is `T * K * bf_bytes`. During the
probe stage, each task holds `S` deserialized bloom filters, so per-executor probe memory is `T * S
* bf_bytes`. Build and probe do not overlap in time for the same bloom filter, so peak GPU pressure
is the larger of the two:

```
GPU_peak_per_executor = T * max(K, S) * bf_bytes
```

`T` is the effective number of concurrent GPU tasks per executor (see §4 for the full definition),
`K` is bloom filter specs per build exec, and `S` is stacked bloom filter predicates per probe-side
scan. At `bf_bytes=8 MB`, `T=16`, and `max(K, S)=3`, peak footprint reaches 384 MB per executor,
which is significant on a 16-24 GB GPU.

**Remedy.** Probe-side bloom filters are wrapped in `SpillableBuffer` at `ACTIVE_ON_DECK_PRIORITY`.
Under GPU memory pressure, RAPIDS can spill bloom filters to host memory and re-materialize them on
demand, reducing steady-state GPU pressure. However, initial deserialization allocates a device
buffer before wrapping, so sufficient GPU memory must be available at deserialization time. In
addition, re-materialization after spill requires a device allocation that can fail under severe
concurrent pressure. `SpillableBuffer` is a mitigation that reduces long-lived GPU memory, not a
guarantee against out-of-memory failures.

Build-side GPU Scalars are not spillable. They are held for the partition lifetime and released at
finalize.

**Safe configuration.** To keep peak GPU footprint under a target budget `G`:

```
bf_bytes_max <= G / (T * max(K, S))
```

For example, to stay under 256 MB on a 16 GB GPU with `T=16` and `max(K,S)=3`: `bf_bytes_max <= 256
MB / 48 = about 5.3 MB`. Setting the planner's size cap at or below 4 MB keeps the footprint under
192 MB with headroom.

For bloom filters capped below 200 KB, the operating point remains below 10 MB per executor even at
`T*max(K,S)=48`.

### 9.2 Network Between Executors and Driver

**Risk.** Build-side accumulator shipping totals `P_build * K * bf_bytes` bytes across all
partitions for all K specs in one build exec. At `bf_bytes=8 MB`, `K=3`, and `P_build=200`, this is
4.7 GB total. The bytes travel as part of Spark task-completion messages, not as separate transfers,
so they share the driver RPC channel with all other accumulator traffic.

Each build task ships K serialized byte arrays, one per spec, as accumulator updates inside
`DirectTaskResult`. These contribute to Spark's task result size accounting
(`spark.driver.maxResultSize`, default 1 GB), which is an aggregate cap across serialized task
results for a job, not a per-task limit. However, large per-task payloads also create RPC pressure
on individual task-completion messages. Per-task accumulator payload is `K * bf_bytes`. At the 256
MB V1 indexing ceiling fallback with `K=3`, worst-case per-task payload reaches 768 MB, which
creates significant RPC pressure. With a planner-set size cap in the low-MB range, per-task payload
is well within normal limits.

**Remedy.** The size guard (`effectiveMaxFilterBytes`) rejects oversized bloom filters before any
GPU allocation or network transfer. The planner's size cap bounds both total network volume and
per-task payload.

**Safe configuration.** For total network budget `N_total` across all K specs:

```
bf_bytes_max <= N_total / (P_build * K)
```

For aggregate task result budget (accounting for `spark.driver.maxResultSize`):

```
P_build * K * bf_bytes <= task_result_budget
```

For per-task RPC payload:

```
K * bf_bytes <= per_task_payload_budget
```

Both budgets should include margin for normal task result values and other accumulators. For
example, to keep total network under 200 MB with `P_build=200` and `K=3`: `bf_bytes_max <= 200 MB /
600 = about 340 KB`. Dimension-side bloom filters, which are the common case, have `P_build` in the
single digits and produce negligible network traffic regardless of bloom filter size.

### 9.3 Driver Heap and Merge CPU

**Risk.** The driver holds one merged `byte[]` per bloom filter id. For `B` total bloom filters
across all concurrent queries (each build exec contributes K bloom filters), the driver heap for
merged arrays is `B * bf_bytes`. Merge is incremental, OR'ing each partition's data section into the
existing array, so peak heap is not `P_build` copies. However, merge runs single-threaded on the
DAGScheduler event loop. At `P_build=200` and `bf_bytes=8 MB`, the driver processes `200 * 8 MB =
1.6 GB` of OR operations per bloom filter. At large filter sizes this becomes visible driver CPU and
GC work. The size cap should keep merge volume within normal operating ranges for typical
dimension-side bloom filters.

**Remedy.** The size guard bounds `bf_bytes`. Standard JVM heap sizing applies for concurrent bloom
filters.

**Safe configuration.** Ensure:

```
driver_heap >= baseline + B * bf_bytes + margin
```

where `B` is the maximum number of bloom filters alive on the driver at once. For a single query
with `K=3` at 1 MB each: 3 MB of additional driver heap, which is negligible. For 5 concurrent build
execs with `K=3` at 8 MB each: `B=15`, requiring 120 MB of additional driver heap.

### 9.4 Diagnostic Accumulator Cache Lifecycle

**Risk.** `CuBFDiagPairMetric` keeps static build/probe `ConcurrentHashMap` caches. If keyed only by
`bfId`, repeated queries in a long-running driver could retain one accumulator object per distinct
bloom filter id indefinitely.

**Lifecycle.** Diagnostic accumulator caches are keyed by `(SQL execution id, bfId)` and cleaned on
`SparkListenerSQLExecutionEnd`. The cleanup removes only RAPIDS' static cache references for
diagnostic build/probe accumulators. It does not touch the inline BF byte accumulator path,
`CuBFRegistry`, or Spark's own completed accumulator/event-log history.

## 10. Configuration Derivation Summary

The following table maps cluster resources to the configuration value to derive. `T` is the
effective number of concurrent GPU tasks per executor (see §4), `K` is specs per build exec, `S` is
stacked predicates per probe scan, `P_build` is build-side partitions, and `B` is total concurrent
bloom filters on the driver.

| Cluster resource | Constraint | Derive |
|------------------|------------|--------|
| GPU memory per executor | `T * max(K, S) * bf_bytes <= GPU_budget` | `bf_bytes_max = GPU_budget / (T * max(K, S))` |
| Network total | `P_build * K * bf_bytes <= network_budget` | `bf_bytes_max = network_budget / (P_build * K)` |
| Task result aggregate | `P_build * K * bf_bytes <= task_result_budget` | `bf_bytes_max = task_result_budget / (P_build * K)` |
| Per-task RPC payload | `K * bf_bytes <= per_task_payload_budget` | `bf_bytes_max = per_task_payload_budget / K` |
| Driver heap | `B * bf_bytes <= heap_budget` | `bf_bytes_max = heap_budget / B` |

The planner or user policy should set the bloom filter size cap to the minimum of these derived
values. Bloom filters above the configured cap should be skipped before reaching GPU allocation or
network transfer.

The RAPIDS execution layer provides a separate fail-closed guard (`effectiveMaxFilterBytes`,
resolved from an optional capability helper or the format-specific indexing ceiling). This guard
rejects bloom filters that exceed the format or capability limit, but it is not intended as the
primary knob for cluster-specific resource budgets. The planner's size cap is the intended
configuration point for GPU, network, per-task payload, and driver heap constraints.

In all dimensions, `bf_bytes` is the shared scaling factor. Small dimension-side bloom filters are
well within safe bounds when capped in the low hundreds of KB. The risk materializes only when large
fact-side bloom filters are permitted, and the planner's size cap exists precisely to prevent that.
