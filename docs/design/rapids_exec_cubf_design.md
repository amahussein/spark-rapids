---
layout: page
title: CuBF GPU Exec Design
nav_order: 18
parent: Developer Overview
---
# CuBF GPU Exec Design

This document covers the following topics:

* [1. Background & Motivation](#1-background--motivation)
* [2. Scope & Planner Contracts](#2-scope--planner-contracts)
  * [2.1 Scope](#21-scope)
  * [2.2 Contracts Expected From the Planner Layer](#22-contracts-expected-from-the-planner-layer)
* [3. Design Goals](#3-design-goals)
* [4. End-to-End Architecture](#4-end-to-end-architecture)
* [5. Component Design](#5-component-design)
  * [5.1 CuBFSpec](#51-cubfspec)
  * [5.2 GpuGenerateCuBFExec](#52-gpugeneratecubfexec)
  * [5.3 InlineCuBFBuildReplacement](#53-inlinecubfbuildreplacement)
  * [5.4 GpuOverrides Registration](#54-gpuoverrides-registration)
  * [5.5 Build Accumulator & Delivery](#55-build-side-accumulator-and-delivery-contract)
  * [5.6 Probe-Side Metadata and Instrumentation](#56-probe-side-metadata-and-instrumentation)
  * [5.7 GpuBloomFilterMightContain Compatibility](#57-gpubloomfiltermightcontain-compatibility)
  * [5.8 Shim Wiring and Supported Profiles](#58-shim-wiring-and-supported-profiles)
* [6. Inertness & Compatibility](#6-inertness--compatibility)
* [7. Resource Management & Failure Behavior](#7-resource-management--failure-behavior)
  * [7.1 Pass-Through Semantics](#71-pass-through-semantics)
  * [7.2 GPU Resource Ownership](#72-gpu-resource-ownership)
  * [7.3 Closing Behavior](#73-closing-behavior)
  * [7.4 Accumulator Merge Behavior](#74-accumulator-merge-behavior)
  * [7.5 Unsafe or Unavailable Bloom Filters](#75-unsafe-or-unavailable-bloom-filters)
* [8. Appendix](#8-appendix)
  * [8.1 Alternatives Considered](#81-alternatives-considered)
  * [8.2 Validation Strategy](#82-validation-strategy)
  * [8.3 Cost & Resource Profile](#83-cost--resource-profile)
  * [8.4 Limitations and Resource Risks](#84-limitations-and-resource-risks)
  * [8.5 Future Work](#85-future-work)

## 1. Background & Motivation

Spark OSS can already inject runtime bloom filters through `InjectRuntimeFilter`. When Spark
produces the standard `BloomFilterAggregate` and `BloomFilterMightContain` expressions, RAPIDS
supports accelerating that path on the GPU. That support remains the baseline behavior: Spark-owned
runtime bloom filter planning continues to work without requiring CuBF-specific nodes.

CUDA bloom filter (CuBF) is a RAPIDS-owned extension point for additional bloom filter opportunities
that are beneficial when the build and probe pipelines are already GPU-resident. The goal is to let
a planner identify opportunities that Spark OSS does not inject, while keeping the RAPIDS execution
layer responsible for GPU resource management, bloom filter construction, delivery, and probe-side
compatibility.

This document describes the RAPIDS GPU execution support for CuBF. It does not define the full
logical optimization policy: join eligibility, benefit estimation, user-facing policy, and marker
generation are separate planner-layer responsibilities.

## 2. Scope & Planner Contracts

### 2.1 Scope

This design covers the RAPIDS-side execution architecture:

- **Build marker replacement:** Detect an optional planner-emitted build marker and replace it
  with a GPU execution node.
- **Inline GPU build:** Build one or more bloom filters while the build-side columnar batches flow
  through unchanged.
- **Delivery contract:** Publish merged bloom filter bytes or a skip state keyed by bloom filter id.
- **Probe compatibility:** Keep `GpuBloomFilterMightContain` compatible with both Spark OSS bloom
  filters and CuBF-produced bloom filters.
- **Shim integration:** Wire the replacement and already-GPU exec registration only in supported
  Spark profiles.
- **Observability hooks:** Provide optional build/probe metadata plumbing without changing
  canonical execution semantics.

### 2.2 Contracts Expected From the Planner Layer

The RAPIDS execution layer expects the planner layer to:

1. Emit a physical build marker or stub only when a CuBF bloom filter should be built.
2. Provide stable bloom filter ids that link build-side specs to probe-side consumers.
3. Provide build specifications that are valid for the build-side output schema.
4. Provide probe-side consumers that interpret missing or skipped bloom filters as no-op filters,
   not as negative membership results.
5. Avoid depending on CuBF execution classes in Spark profiles where CuBF is not supported.

## 3. Design Goals

- **Inert by default:** Normal RAPIDS planning is unchanged unless CuBF planner nodes are present.
- **Pass-through execution:** Build-side rows, schema, and batch objects flow through unchanged.
- **Inline construction:** Bloom filters are built during the existing build-side GPU pass,
  avoiding an extra build-side scan.
- **Multi-filter support:** One build node can construct multiple bloom filters over the same
  child stream.
- **Safe delivery:** Consumers can distinguish real bloom filter bytes from a skip/no-op state.
- **OSS compatibility:** Existing `InjectRuntimeFilter` acceleration remains unchanged.
- **Profile isolation:** Unsupported Spark profiles do not need CuBF-specific planner classes or
  execution classes.

## 4. End-to-End Architecture

At a high level, the logical/planner layer owns the decision to create CuBF markers. RAPIDS owns
replacing those markers with GPU execution and preserving safe behavior when markers are absent.

```
Logical / Planner Layer
  choose opportunity -> emit build marker/stub (bfId + CuBFSpec)
        -> emit probe consumer (same bfId)
        |  physical plan
        v
RAPIDS Replacement Layer  (preColumnarTransitions -> applyPreGpuOverridesRules -> GpuOverrides)
  InlineCuBFBuildReplacement: reflectively detect marker, read CuBFSpec(s),
        produce GpuGenerateCuBFExec
        |  GPU physical plan
        v
Build-Side GPU Pipeline
  child batches -> GpuGenerateCuBFExec -> same child batches
                     +-- build per-partition bloom filters
                     +-- publish bytes or skip state by bfId
        |  delivery contract
        v
Probe-Side Consumption
  bloom filter bytes / no-op -> GpuBloomFilterMightContain (Spark-compatible bytes)
```

The execution layer is deliberately narrow: it does not decide whether a join should receive a bloom
filter, and it does not assume the planner layer is always available on the classpath.

## 5. Component Design

### 5.1 `CuBFSpec`

`CuBFSpec` is the per-bloom-filter build specification copied from the optional planner marker into
RAPIDS execution. It contains:

| Field | Meaning |
|-------|---------|
| `bfId` | Stable id that links build delivery to probe consumption. |
| `keyColumnIndex` | Build-side output column index used as the bloom filter key. |
| `numHashes` | Number of bloom filter hash functions. |
| `numBits` | Bloom filter bit count. |

The build exec receives one or more specs: a planner may coalesce sibling bloom filter builds that
share the same child stream (for example, one filtered dimension stream feeding both a `customer_id`
and an `order_id` filter into a single `GpuGenerateCuBFExec` with two specs). Shared wire-format and
hashing parameters (`bfVersion`, `seed`, `xxHashSeed`) are carried by the build exec, not duplicated
per spec.

The current execution contract is single-key-column: each `CuBFSpec` points at one build-side key
column. Composite-key support should extend the spec and probe contracts deliberately rather than
overloading `keyColumnIndex`; build and probe execution must first agree on row-level composite
hashing, null handling, and type compatibility before a multi-column key can share the single-key
path's correctness guarantees.

Key invariants:

1. `specs` is non-empty.
2. Every `bfId` is stable and unique for the bloom filter it identifies.
3. `keyColumnIndex` references a build-side column that can be hashed with the probe-side bloom
   filter semantics.
4. All partition-local bloom filters for a given `bfId` have the same serialized size and header
   layout.

Two additional invariants are owned by the planner layer and are required for canonicalization
safety in §5.2:

5. **Content-addressed `bfId`s.** Two build operations over the same canonicalized child producing
   equivalent specs must use the same `bfId`. This makes ReuseExchange collapses safe: one
   accumulator keyed by `bfId` serves all probe-side consumers that reference it. CTE replays of the
   same filtered dimension are the canonical case.
6. **Sibling-coalescence pre-pass.** Distinct specs over the same build child are coalesced into a
   single multi-spec marker before `InlineCuBFBuildReplacement` runs. Two `InlineCuBFBuildExec`
   nodes with different specs and equal canonical children are never emitted.

### 5.2 `GpuGenerateCuBFExec`

`GpuGenerateCuBFExec` is a unary `GpuExec` that builds bloom filters inline with the build-side GPU
pipeline.

Shared build-exec parameters:

- `bfVersion` (`Int`): Bloom filter format version. Version 1 is used by Spark 3.x; version 2 is
  used by Spark 4.x and carries an additional seed field in the serialized header.
- `seed` (`Int`): Bloom filter hash seed used by version 2 serialized bloom filters; version 1
  uses `0`.
- `xxHashSeed` (`Long`): XxHash64 seed used for build keys. This must match the probe-side hash
  used by `BloomFilterMightContain`.
- `buildCostUpdaters` (`Map[String, CuBFDiagPairMetric]`): Optional per-`bfId` build-cost update
  sinks. The default is `Map.empty`, keeping build-cost instrumentation inert.

The operator is pass-through and delegates canonicalization to its child, preserving plan equality
and exchange/subquery reuse; this is safe because invariants 5–6 (§5.1) guarantee any two wrappers
that canonicalize equal already share the same `bfId` and accumulator, so collapsing them is
intended. Stream, GPU-resource-ownership, and closing semantics are covered in §7.1–7.3.

Build behavior:

1. Each task lazily creates a GPU bloom filter per spec as data arrives.
2. Build keys are hashed with the same XxHash64 seed expected by the probe-side
   `BloomFilterMightContain` expression.
3. Partition-local bloom filters are copied to Spark-compatible serialized bytes.
4. One build accumulator per `bfId` receives the partition-local bytes.
5. Empty partitions do not publish bloom filter bytes.

Safety behavior:

- Oversized bloom filters are marked skipped before launching GPU build work.
- The effective size cap is resolved through an optional capability helper when available; failure
  to resolve it falls back to a conservative cap.

### 5.3 `InlineCuBFBuildReplacement`

`InlineCuBFBuildReplacement` is a pre-`GpuOverrides` physical rule that finds an optional
planner-emitted inline build marker and replaces it with `GpuGenerateCuBFExec`. The shim entry point
runs a fast class-name scan (`isNeeded`) before instantiating the rule, so plans without CuBF
markers bypass rule construction entirely. When runtime-feedback instrumentation is enabled, the
rule also resolves per-`bfId` build-cost updaters and passes them to the exec (§5.6).

Detection is reflection-based to avoid a compile-time dependency on planner-layer classes: no marker
present returns the original plan; a marker with readable fields becomes `GpuGenerateCuBFExec`; a
marker present but unreadable returns the original marker unchanged. The preferred marker exposes a
`specs` sequence; a legacy single-spec shape is adapted into a one-element `Seq[CuBFSpec]` to keep
the downstream contract uniform.

If replacement cannot be performed the rule leaves the plan unchanged. The planner layer must ensure
any unreplaced marker has safe no-op behavior or is not emitted in unsupported environments.

### 5.4 `GpuOverrides` Registration

`GpuGenerateCuBFExec` is produced already-GPU, but a pass-through `GpuOverrides` registration
(`InlineCuBFBuildGpuOverride`) is still needed for the normal planning pass to recognize it: tagging
accepts it and conversion returns the existing node unchanged.

### 5.5 Build-Side Accumulator and Delivery Contract

`CuBFBuildResultAccumulator` is the build-side delivery primitive: one accumulator per `bfId`.
Partition-local bloom filter bytes are merged on the driver by bitwise OR over the serialized data
section; oversized, unsafe, or partially-built builds publish a skip sentinel instead of bytes.

Merge invariants:

1. Headers for a given `bfId` are identical across partitions.
2. Only the data section is OR-merged; the header is preserved.
3. Serialized bloom filter versions with different header sizes are handled by detecting the version
   header. Version 1 uses a 12-byte header; version 2 uses a 16-byte header that includes an
   additional 4-byte hash seed field.
4. A size mismatch is a contract violation and must not be silently merged.

Skip behavior uses a four-byte all-zero sentinel. The sentinel is not a valid serialized bloom
filter because real bloom filter payloads start with a non-zero version header. Once any partition
or driver-side guard publishes the sentinel, skip wins over later real bytes. Downstream delivery
consumers must interpret the sentinel as "do not apply this bloom filter".

The delivery contract has three meaningful states:

- **Merged bytes:** A valid bloom filter was built. Publish bytes to the probe predicate.
- **Skip sentinel:** The bloom filter should not be used. Apply no bloom filter for this `bfId`.
- **No bytes:** No partition produced a filter. Treat as no-op unless the planner contract says
  otherwise.

### 5.6 Probe-Side Metadata and Instrumentation

The core probe expression remains `GpuBloomFilterMightContain`. CuBF adds optional, diagnostic-only
metadata around it, defaulting to absent:

- `bfId` identifies the CuBF bloom filter associated with a probe predicate.
- `CuBFDiagPairMetric.probeForBfId` aggregates per-batch `(rowsIn, rowsPassed)` updates on the
  driver; `CuBFDiagPairMetric.buildForBfId` aggregates build-side `(buildWallNanos, bfBytes)`
  updates.

These hooks are enabled only when `spark.rapids.sql.cubloomfilter.diagnosticMetrics.enabled=true`
and the `bfId` can be discovered from a planner-emitted CuBF marker. Otherwise no diagnostic
accumulator is registered and the probe path pays no per-batch reduction, device-to-host scalar
copy, or accumulator update.

Discovery walks the probe-side bloom filter subquery plan for a planner-emitted node carrying the
`bfId`, identified by class name and read reflectively (no compile-time dependency on planner
classes). Because AQE can wrap the subquery in `AdaptiveSparkPlanExec`, discovery also reaches the
underlying physical plan through AQE plan accessors reflectively. If discovery fails at any step,
`bfId` and `probeUpdater` stay `None` and the expression behaves as the standard OSS replacement.

Diagnostic accumulators are keyed by `(SQL execution id, bfId)` so repeated `bfId`s stay
distinguishable across executions, and are released on `SparkListenerSQLExecutionEnd` (companion
§9.4).

Instrumentation must not change execution semantics:

1. Probe updates are per columnar batch, not per row; build updates are per bloom filter build, not
   per input batch.
2. Canonicalization drops `bfId` and updater fields so observability wiring does not change plan
   equivalence.
3. When metadata is absent, `GpuBloomFilterMightContain` behaves like the existing Spark OSS bloom
   filter expression replacement.

### 5.7 `GpuBloomFilterMightContain` Compatibility

`GpuBloomFilterMightContain` consumes Spark-compatible serialized bloom filter bytes and probes a
`LongType` value column on the GPU. CuBF-produced bloom filters must use the same serialized wire
format and the same hash semantics as the existing Spark OSS path.

Compatibility expectations:

- CuBF skip/no-op state must be handled before bytes reach `GpuBloomFilterMightContain`.
- Optional CuBF metadata does not affect type checking, canonicalization, or the existing OSS
  `BloomFilterMightContain` replacement path.

### 5.8 Shim Wiring and Supported Profiles

CuBF execution support is wired through the Spark shim layer: a generic shim hook allows
pre-`GpuOverrides` physical rules; supported bloom filter profiles apply
`InlineCuBFBuildReplacement` before the normal override pass and register `GpuGenerateCuBFExec` as
an already-GPU operator; profiles without Spark runtime bloom filter support keep empty or no-op
shims. A profile should enable CuBF execution only when it supports the required Spark bloom filter
expressions, serialized format, and RAPIDS JNI bloom filter primitives; unsupported profiles should
either never see CuBF planner markers or leave them with safe no-op behavior.

Current profile behavior is split into three tiers:

- **Pre-3.3.0 profiles:** No Spark bloom filter expression support is registered.
- **Databricks shim variants:** Spark bloom filter expression support is retained, but CuBF build
  exec wiring and pre-`GpuOverrides` replacement are not enabled. These profiles fall through to
  the base shim identity and remain inert for CuBF build markers.
- **OSS Spark 3.3.0+ profiles:** Spark bloom filter expression support,
  `InlineCuBFBuildReplacement`, and `GpuGenerateCuBFExec` registration are enabled.

The canonical source for supported profiles is the `spark-rapids-shim-json-lines` annotation header
in `GpuGenerateCuBFExec.scala`.

## 6. Inertness & Compatibility

The design is intentionally inert unless the planner layer emits CuBF nodes: with no marker in the
plan, the pre-`GpuOverrides` quick check returns the original plan, normal RAPIDS `GpuOverrides`
planning proceeds, and existing Spark OSS bloom filter support is unchanged.

Compatibility properties:

- There is no compile-time dependency on optional planner marker classes.
- Marker detection is based on an optional class name and reflective accessors.
- If marker classes are absent, no replacement occurs and normal planning continues.
- If optional probe metadata is absent, constructor defaults preserve existing
  `GpuBloomFilterMightContain` behavior.
- Existing Spark OSS `InjectRuntimeFilter` support remains unchanged: standard Spark
  `BloomFilterAggregate` and `BloomFilterMightContain` nodes still follow the established RAPIDS
  acceleration path.
- Unsupported Spark profiles can keep no-op shim hooks and do not need CuBF-specific planner classes
  on the classpath.

## 7. Resource Management & Failure Behavior

### 7.1 Pass-Through Semantics

`GpuGenerateCuBFExec` must not change the build-side data stream. Its child batches are returned to
the parent operator unchanged, and the output schema is the child schema. This property is required
so adding a CuBF build node does not affect join build-side semantics.

### 7.2 GPU Resource Ownership

The build exec owns GPU bloom filter scalars it creates. Temporary hashed columns and host buffers
used to serialize bloom filters are closed with RAPIDS resource helpers. The probe expression owns
the deserialized `GpuBloomFilter` it creates from a bloom filter scalar and closes it on task
completion. The probe-side `GpuBloomFilter` wraps its device buffer in a `SpillableBuffer`, so the
deserialized bloom filter participates in RAPIDS memory spilling under GPU memory pressure.

### 7.3 Closing Behavior

Normal completion finalizes all bloom filters when the input iterator is exhausted. A task
completion listener closes any still-live GPU bloom filters if the task fails, is interrupted, or
stops consuming the iterator before normal exhaustion. Closing is best-effort and must not mask the
original task failure.

A partition whose iterator is abandoned before normal exhaustion (for example a downstream `LIMIT` /
`take(N)` pushdown) has an incomplete build, so the listener publishes the skip sentinel for that
partition's bloom filter rather than its partial bytes. Because skip is absorbing (§7.4), that
`bfId` merges to skip and the probe falls back to a no-op filter — partial drains fail closed and
cannot cause false negatives.

### 7.4 Accumulator Merge Behavior

Accumulator merge is a driver-side reduction over serialized partition-local bloom filters. Real
bloom filter bytes merge by OR-ing the data section. The skip sentinel is absorbing: once present,
the accumulator value remains skipped. This makes oversize and unsafe-build decisions deterministic
across partition merge order.

### 7.5 Unsafe or Unavailable Bloom Filters

If the execution layer can determine that a bloom filter cannot be built safely, it publishes
skip/no-op state instead of partial bytes. Examples include an effective size cap exceeded before
kernel launch or an unavailable optional capability helper causing the guard to fail closed.

Unexpected runtime failures follow normal Spark task failure behavior. Resource cleanup still runs,
and consumers must not treat missing or skipped CuBF output as evidence that probe-side rows are
absent. The safe outcome is to skip the CuBF filter and preserve query correctness.

## 8. Appendix

This appendix collects supporting material — design rationale, the validation matrix, pointers to
the cost and resource-risk analysis, and planned extensions. It is contextual to cuBF's introduction
and is expected to be pruned as cuBF matures in the codebase; the durable design is §1–§7.

### 8.1 Alternatives Considered

**Reuse the Spark OSS scalar-subquery build path.** Kept and still supported, but reusing only that
path would limit CuBF to opportunities Spark already represents and can require a separate
scalar-subquery build pipeline. Inline build gives a narrower execution contract for
planner-selected GPU opportunities.

**Hard-link against planner-layer classes.** Direct type references would simplify replacement code
but force the public execution module to compile and run with planner-layer classes present.
Reflection keeps the layer independently loadable and lets unsupported profiles remain no-op.

**Change existing OSS bloom filter behavior globally.** Rejected: changing
`BloomFilterAggregate`/`BloomFilterMightContain` globally risks regressions in Spark-owned
`InjectRuntimeFilter` queries. CuBF instead adds optional metadata and a separate inline build path,
preserving the established OSS path.

**Build bloom filters with an extra build-side scan.** Rejected: an extra scan increases I/O and GPU
work and can interfere with exchange/subquery reuse. Inline construction reuses the build-side
stream already flowing through the GPU pipeline.

### 8.2 Validation Strategy

Validation should cover both execution contracts and inertness:

- **Accumulator merge:** Real bloom filter bytes OR correctly, headers are preserved, and size
  mismatches are rejected.
- **Skip/no-op behavior:** The skip sentinel wins over real bytes and is distinguishable from valid
  bloom filter bytes.
- **Multi-spec build contract:** One build node registers one accumulator per spec and avoids
  cross-contamination between `bfId` values.
- **Replacement reflection:** Test-only stubs exercise multi-spec and legacy single-spec marker
  shapes without optional classpath dependencies.
- **Already-GPU planning:** `GpuGenerateCuBFExec` survives `GpuOverrides` as an already-GPU node.
- **Inert planning:** Plans with no CuBF markers are unchanged by pre-`GpuOverrides` rules.
- **Probe metadata:** Optional `bfId` and updater wiring does not affect canonicalization or normal
  predicate behavior.
- **Spark OSS regression:** Existing runtime bloom filter tests for `InjectRuntimeFilter` continue
  to pass unchanged.

End-to-end query tests should be added when the logical/planner layer is present in the public test
environment. Until then, execution tests should keep using public or test-only marker stubs.

### 8.3 Cost & Resource Profile

CuBF adds overhead in three places — executor GPU memory (build and probe), executor-to-driver
network for the shipped accumulator bytes, and driver heap plus merge CPU — and every dimension
scales linearly with the per-filter size `bf_bytes`. Network (`P_build * K * bf_bytes`) is the first
dimension to matter, because it multiplies by the build-partition count. A size guard caps
`bf_bytes` at `effectiveMaxFilterBytes` (an optional capability helper, falling back to the ~256 MB
V1 indexing ceiling) and skips oversized filters before any GPU allocation or network transfer, so
worst-case overhead is bounded; for typical dimension-side filters (1 KB–200 KB) every dimension is
negligible.

The full cost model — per-task and total overhead, the sensitivity tables, the worked sizing
example, and the cost-telemetry caveats — is in the companion analysis doc:
[CuBF GPU Exec Cost & Sizing Analysis](rapids_exec_cubf_cost_and_sizing.md).

### 8.4 Limitations and Resource Risks

The byte-sized resource risks — executor GPU memory, executor-to-driver network, and driver heap and
merge CPU — are bounded by the `bf_bytes` size guard (§8.3) and standard executor/driver sizing. The
diagnostic-accumulator cache lifecycle is bounded instead by `(SQL execution id, bfId)` keying and
cleanup on `SparkListenerSQLExecutionEnd` (§5.6). None of these changes query correctness. The
per-dimension worst-case numbers, their mitigations, and the configuration-derivation guidance are
in the companion analysis doc:
[CuBF GPU Exec Cost & Sizing Analysis](rapids_exec_cubf_cost_and_sizing.md).

### 8.5 Future Work

The execution layer is designed to support the following extensions without changing existing Spark
OSS bloom filter behavior:

1. Logical injection rules that decide where CuBF markers should appear.
2. Join eligibility and benefit heuristics.
3. User-facing configuration for enabling CuBF policy, where appropriate.
4. Planner-side build and probe marker generation.
5. End-to-end query integration tests that require logical planning support.
6. Composite-key bloom filter support for multi-column join keys, including stable build/probe
   composite hashing, null semantics, type support, and marker/spec shape.
7. Additional selectivity gating or cost-benefit policies.
8. Additional profile enablement as Spark bloom filter support and shim coverage evolve.
