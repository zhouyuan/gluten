---
layout: page
title: Gluten Extension Overlay Design
nav_order: 100
parent: Developer Overview
---

# Gluten Extension Overlay: A Library of Gluten-Managed Functions and Operators

Status: DRAFT / Proposal

Related work:

* [#12817](https://github.com/apache/gluten/pull/12817) — Function overlay (the function half of this design)
* [#12739](https://github.com/apache/gluten/pull/12739) — Example custom operator (GlutenStride, to be reworked onto this design)
* [#12773](https://github.com/apache/gluten/pull/12773) — Inventory of the Velox API surface used by Gluten
* [#12456](https://github.com/apache/gluten/issues/12456) / [#12454](https://github.com/apache/gluten/pull/12454) — Bolt backend integration

## 1. Motivation

Gluten today is mostly a *translator*: Spark plans are converted to Substrait IR and handed to a
native engine (Velox, ClickHouse, and soon Bolt) that owns all operator and function
implementations. When an engine implementation is missing or diverges from Spark semantics,
Gluten's options are to fall back to vanilla Spark or to wait for an upstream fix, both of which
couple Gluten's release cadence and its correctness/performance story to upstream engines.

This design gives Gluten a third option: **a curated library of Gluten-managed function and
operator implementations** that plug into the Substrait-based IR, so that the final executed plan
is a free mix of implementations from the engine (Velox/Bolt) and from Gluten itself:

```
Spark physical plan
   │  offload rules: per node, pick { Gluten library op | engine op | vanilla Spark fallback }
   ▼
Substrait IR  ──  standard rels                    (→ engine operators)
              ──  ExtensionSingleRel + Gluten proto (→ Gluten library operators)
              ──  function calls by name            (→ resolved from the unified registry)
   ▼
Native plan conversion (per-backend converter registry)
   ▼
One engine plan tree: engine nodes and Gluten nodes interleaved
   ▼
Engine Task/Driver executes mixed pipelines
```

Two properties make the mixing cheap:

1. Gluten operators are real engine operators (`velox::exec::Operator` subclasses), so the driver
   composes them into pipelines with engine operators with zero glue.
2. Functions bind late, by name, in the engine's registry, so a plan freely mixes Gluten and
   engine functions per call site.

## 2. What exists today

* **Function overlay** (#12817): `cpp/velox/operators/functions/overlay/` hosts Velox-compatible
  function implementations. `registerFunctionOverlay()` runs at the end of
  `registerAllFunctions()`; because Velox registries let a later same-name, same-signature
  registration replace an earlier one, overlay functions take precedence over Velox's. `round` is
  the first entry.
* **Scattered custom operators**: Gluten already ships custom Velox operators —
  `ValueStream`/`RowVectorStream`, `HashTableBuilder`, `CudfVectorStream` — each wired up
  ad-hoc (bespoke rel encodings, direct `Operator::registerOperator` calls in
  `VeloxBackend::init`, per-operator branches in `SubstraitToVeloxPlan.cc`).
* **Marker-based rel overloading**: custom semantics ride on standard rels via
  `AdvancedExtension.optimization` markers (e.g. `isSMJ=` on JoinRel; the GlutenStride draft uses
  `isGlutenStride=` on FetchRel). This works but scatters dispatch, confuses the native validator
  (which validates the host rel's semantics, not the real operator's), and misleads anything that
  inspects plans.

The extension-point surface Gluten relies on is documented in
[velox-api-usage.md](./velox-api-usage.md) (#12773); the table "Where Gluten implements Velox
interfaces" is exactly the contract this design builds on.

## 3. Design overview

The overlay is split along a deliberate seam:

| Half | Contents | Location | Backend coupling |
|---|---|---|---|
| **IR contract** | detail protos, `RelBuilder.makeExtensionRel`, transformer/offload-rule conventions, config gating | `gluten-substrait` + per-backend Scala | none — pure Substrait |
| **Engine implementation** | converter registry, `PlanNodeTranslator` registrations, operator/function C++ | `cpp/velox` (and later `cpp` of other backends) | per backend |

Overlay operators are therefore **portable by contract, not by code**: an operator's rel is
defined once in the IR; each backend that supports it registers a converter for its type URL and
provides an implementation. A backend that doesn't support it fails validation for that rel and
the planner falls back.

The two halves and their composition at runtime:

```
┌─ Spark (JVM) ────────────────────────────────────────────────────────────────┐
│ SQL / DataFrame ──► Catalyst ──► Spark physical plan                         │
│                         │                                                    │
│                         ▼                                                    │
│ offload rules — per node, config-gated choice:                               │
│     { Gluten overlay op | engine op | vanilla Spark fallback }               │
│                                                                              │
│ overlay *ExecTransformer                 standard *ExecTransformers          │
│     │  doTransform →                         │                               │
│     │  makeExtensionRel                      │                               │
│     ▼                                        ▼                               │
│ ExtensionSingleRel +                     standard Substrait rels             │
│ Gluten detail proto (Any)                    │                               │
│     └───────────────────┬────────────────────┘                               │
│                         ▼                                                    │
│              whole-stage Substrait plan                                      │
│                                                                              │
│ [IR contract half — gluten-substrait + per-backend Scala, engine-neutral]    │
└──────────────────────────────────────────────────────────────────────────────┘
                          │
                          │ JNI
                          ▼
┌─ native backend (cpp/velox today; Bolt/CH analogous) ────────────────────────┐
│ SubstraitToVeloxPlanConverter / SubstraitToVeloxPlanValidator                │
│     ├─ standard rels ───────────────────► engine plan nodes                  │
│     └─ extension_single ─► rel-handler registry ─► Gluten plan nodes         │
│                            (keyed by detail type URL; unknown URL            │
│                             ⇒ validation failure ⇒ planner falls back)       │
│                                                                              │
│ function calls, by name ──► engine function registry                         │
│                             (function overlay registers last and wins)       │
│                                                                              │
│ Task/Driver composes one pipeline from the mixed plan tree, zero glue:       │
│     TableScan ── Filter ── GlutenSample ── HashAggregate                     │
│      (engine)    (engine)   (Gluten op)      (engine)                        │
│                                                                              │
│ [engine implementation half — per backend]                                   │
└──────────────────────────────────────────────────────────────────────────────┘
```

## 4. Function overlay (recap)

Unchanged from #12817. One addition from this design: each entry gets a lifecycle tag and,
where it overrides an engine function, an entry in the manifest (Section 7) so precedence
collisions are visible at init time.

## 5. Operator overlay

An operator overlay entry spans four layers. Adding one touches **only additive, per-operator
files** — no shared dispatch code is edited.

End to end, one entry looks like this (Sample as the running example):

```
Spark operator (e.g. SampleExec)
    │  offload rule, gated by spark.gluten.sql.overlay.sample.enabled
    ▼
┌─ Scala transformer (§5.2) — backends-velox/.../execution/overlay/ ───────────┐
│ SampleExecTransformer: doValidateInternal → doNativeValidation               │
│                        doTransform        → RelBuilder.makeExtensionRel      │
└──────────────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─ wire encoding (§5.1) — org/apache/gluten/proto/ ────────────────────────────┐
│ ExtensionSingleRel{ input, detail = Any(GlutenSampleRel{...}) }              │
│ type URL "type.googleapis.com/gluten.GlutenSampleRel" = identity             │
└──────────────────────────────────────────────────────────────────────────────┘
    │  inside the whole-stage Substrait plan, across JNI
    ▼
┌─ native conversion (§5.3) — cpp/velox/operators/overlay/ ────────────────────┐
│ rel-handler registry: type URL → { convert(), validate() }                   │
│ SampleRelHandler.convert → core::PlanNodePtr (SampleNode)                    │
└──────────────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─ engine operator (§5.4) — cpp/velox/operators/overlay/Sample/ ───────────────┐
│ SampleNode + SampleOperator (velox::exec::Operator) + PlanNodeTranslator,    │
│ registered once by registerOperatorOverlay() in VeloxBackend::init           │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 5.1 Wire encoding: `ExtensionSingleRel` + Gluten detail proto

Substrait already provides the extension mechanism:
`ExtensionSingleRel { RelCommon common; Rel input; google.protobuf.Any detail; }`, reachable as
`Rel.extension_single` (already present in Gluten's vendored `algebra.proto`; unused today).

Each operator defines a small detail message in the existing Gluten proto directory
(`backends-velox/src/main/resources/org/apache/gluten/proto/`, compiled on both the Java side and
the C++ side via `GLUTEN_PROTO_SRC_DIR`; the Iceberg extensions are precedent for packing Gluten
protos into Substrait `Any`):

```protobuf
// gluten_sample_rel.proto
message GlutenSampleRel {
  double lower_bound = 1;
  double upper_bound = 2;
  bool with_replacement = 3;
  int64 seed = 4;
}
```

The `Any` type URL (`type.googleapis.com/gluten.GlutenSampleRel`) is the operator's identity on
the wire, used for dispatch on the native side.

We do **not** overload standard rels with `AdvancedExtension` markers for new operators.
`AdvancedExtension.enhancement` on the `ExtensionSingleRel` remains available for the
validation-mode input-type annotation, same as other rels.

### 5.2 Scala/Java layer

* One generic builder in `gluten-substrait`:

  ```java
  RelBuilder.makeExtensionRel(RelNode input, com.google.protobuf.Any detail,
      AdvancedExtensionNode extensionNode /* nullable */, SubstraitContext ctx, Long operatorId)
  ```

  Per-operator `make*Rel` methods are not added to the shared `RelBuilder`.
* One `*ExecTransformer` per operator in the backend module (e.g.
  `backends-velox/.../execution/overlay/`), following the standard `UnaryTransformSupport`
  pattern: `doValidateInternal` → `doNativeValidation`, `doTransform` → `makeExtensionRel`.
* An offload path. Two patterns:
  * **Rule-driven** (the normal case): an offload rule maps a Spark operator to the transformer,
    gated by config (Section 7). The operator is then embedded mid-stage in the whole-stage
    Substrait plan like any other transformer.
  * **Directly constructed** (tests, mechanism demos): the transformer is instantiated
    programmatically. GlutenStride is this kind.

### 5.3 Native conversion: converter registry

`SubstraitToVeloxPlanConverter::toVeloxPlan(Rel&)` gains exactly one new branch —
`rel.has_extension_single()` — which dispatches through a registry keyed by the detail's type URL:

```cpp
// cpp/velox/operators/overlay/OperatorOverlay.h
struct OverlayRelHandler {
  // Build the engine plan node from the detail proto.
  std::function<core::PlanNodePtr(const google::protobuf::Any& detail,
                                  core::PlanNodePtr child,
                                  SubstraitToVeloxPlanConverter& ctx)> convert;
  // Validation-mode check; unknown type URLs fail validation => planner falls back.
  std::function<bool(const google::protobuf::Any& detail,
                     SubstraitToVeloxPlanValidator& ctx,
                     std::string& reason)> validate;
};
void registerOverlayRelHandler(const std::string& typeUrl, OverlayRelHandler handler);
```

`SubstraitToVeloxPlanValidator` gains the mirror `extension_single` branch, delegating to
`validate`. An unregistered type URL is a validation failure, never a crash — the safe default is
fallback.

### 5.4 Operator registration

Mirrors the function overlay exactly: `registerOperatorOverlay()` in
`cpp/velox/operators/overlay/` registers every entry's `PlanNodeTranslator` and rel handler, and
is called once from `VeloxBackend::init` (replacing today's direct
`Operator::registerOperator(...)` calls, matching the `CudfVectorStreamOperatorTranslator`
precedent).

Each operator is one self-contained module:

```
cpp/velox/operators/overlay/
  README.md
  RegisterOperatorOverlay.{h,cc}      # registers all translators + rel handlers
  OperatorOverlay.{h,cc}              # the rel-handler registry
  Sample/
    SampleNode.h                      # PlanNode + Operator + Translator
    SampleOperator.cc
    SampleRelHandler.cc               # convert + validate for GlutenSampleRel
```

### 5.5 Overriding built-in engine operators (future)

The mechanism above covers *new* operators. Overriding an operator the engine already has (e.g. a
Gluten-fixed Window) cannot go through `PlanNodeTranslator`: Velox's driver factory matches
built-in plan node types before consulting custom translators, so a standard `core::WindowNode`
always gets Velox's operator. The override must happen at plan-construction time via a distinct
node type.

Planned mechanism (not in the first milestone): a **post-conversion rewrite registry** — after
`toVeloxPlan` produces the tree, node-type-keyed rewrites (`WindowNode → GlutenWindowNode`)
registered by the overlay run over it. This preserves the function-overlay property ("overlay
wins over the engine") without touching per-rel conversion code, and removing a rewrite when the
fix lands upstream is a one-line revert. Deferred until there is a concrete override candidate.

## 6. Library lifecycle

Every overlay entry (function or operator) carries a tag:

* **`staging`** — destined for upstream (Velox/Bolt). Removed from the overlay in the same PR
  that bumps the engine version once accepted upstream. The overlay must not silently become a
  fork.
* **`optimized`** — permanently Gluten-owned, with a stated reason. Typical reasons: Spark-exact
  semantics the engine won't take (e.g. `round`, Spark's XORShiftRandom sampling), or
  Spark-workload-specific optimizations.

Review bar: `optimized` entries should be whole, self-contained operators or functions — not
forked variants of complex engine operators (HashJoin, Window internals). Every permanent entry
is code Gluten maintains against a moving engine API; the extension-point table in
[velox-api-usage.md](./velox-api-usage.md) is the breakage surface on version bumps and should
grow deliberately. Operator entries must additionally satisfy the memory contract in
Section 10.1.

## 7. Selection, config gating, and observability

When both Gluten and the engine can implement a node, something must decide which runs:

* **Operators**: the Scala offload rule decides whether to emit the extension rel or the standard
  rel, gated per entry:

  ```
  spark.gluten.sql.overlay.<name>.enabled   (e.g. spark.gluten.sql.overlay.sample.enabled)
  ```

  Fallback order for an `optimized` entry: Gluten-native → standard engine rel (or existing
  emulation) → vanilla Spark. This gives instant production rollback without a rebuild.
* **Functions**: the registry is global and populated at `VeloxBackend::init`, where
  `backendConf_` is available — the same config decides at init time whether an overlay function
  registers over the engine's. Coarser than per-query, but sufficient.

**Manifest**: registration produces a runtime manifest — `name → {kind, tag, config key, which
implementation won}` — logged at init and dumpable for docs/debugging. Combined with the naming
convention that overlay plan nodes keep a `Gluten` prefix (`GlutenSample` in `EXPLAIN`, task
stats, Spark UI), a mixed plan's provenance is always answerable: *which implementation ran?*

## 8. Worked examples

### 8.1 GlutenStride — minimal mechanism walkthrough (#12739, reworked)

Keeps every N-th row per batch. No Spark counterpart, so it is *directly constructed* in tests
and forms its own stage — the smallest possible code touching all four layers, ideal for the
developer guide. Rework from the current draft: replace the `FetchRel{offset=stride} +
isGlutenStride=1` marker encoding with `ExtensionSingleRel + GlutenStrideRel{stride}`, and move
registration into `registerOperatorOverlay()`.

### 8.2 GlutenSample — first real, stage-embedded library entry

Gluten currently offloads `SampleExec` by *emulation*: `SampleExecTransformer` rewrites it to
`FilterRel(rand(seed) < fraction)`. Known gaps (documented in the transformer itself): Spark's
Bernoulli sampler uses XORShiftRandom seeded with `seed + partitionId`, so the emulation is not
row-for-row reproducible against vanilla Spark; and `withReplacement = true` (Poisson sampling)
cannot offload at all.

`GlutenSampleNode` fixes both natively and exercises everything GlutenStride can't:

* **Rule-driven and stage-embedded**: `df.sample(fraction)` / `TABLESAMPLE` → offload rule →
  transformer → the operator lands mid-stage (`Scan → Filter → GlutenSample → Aggregate` in one
  whole-stage Substrait plan) — the mixed Gluten+engine pipeline this design is for. Tests are
  plain SQL/DataFrame queries.
* **Genuinely `optimized`**: Spark-exact XORShiftRandom with `seed + partitionId` (partition id
  is available natively via the Spark query config, the same channel `rand` /
  `monotonically_increasing_id` use), plus the with-replacement path, removing a fallback.
* **Still simple**: unary, streaming, per-batch selection with tiny RNG state; no spill or
  shuffle interaction.
* **Demonstrates selection**: `spark.gluten.sql.overlay.sample.enabled` switches between
  Gluten-native sample, the filter emulation, and vanilla Spark.

## 9. Multi-engine support: Bolt (and CH)

The community is integrating [Bolt](https://github.com/bytedance/bolt) (#12454, #12456) — a
Velox-derived, now independently evolving engine (operator fusion, expression JIT, adaptive
parallelism) — as a separate `backends-bolt` module compiled against the external Bolt library.
The IR/engine split in Section 3 is what makes the overlay carry over:

* **The IR half is engine-neutral and shared.** Detail protos, `makeExtensionRel`, transformer
  conventions, and config gating live in `gluten-substrait` and cost Bolt (and CH) nothing to
  adopt. The common-layer extraction in #12456 should include this half so `backends-bolt` does
  not copy it.
* **Gluten overlay entries on Bolt**: if Bolt preserves Velox's extension APIs
  (`PlanNode`/`Operator`/`PlanNodeTranslator`, function registries with last-wins semantics),
  Gluten's overlay C++ recompiles against Bolt unchanged. The compatibility question is decided
  by exactly the interfaces in [velox-api-usage.md](./velox-api-usage.md)'s extension-point
  table — that table doubles as the **Bolt compatibility checklist**. If Bolt's APIs drift, the
  fallback is a per-backend implementation of the same type URL: the IR contract holds even when
  the code doesn't port.
* **Bolt-only features**: features needing plan-level representation ship as overlay entries
  whose rel handler is registered only in the Bolt backend. Engine-internal features (fusion,
  JIT) live below the IR and apply transparently. The overlay thus becomes the general mechanism
  for surfacing *any* engine's extra capabilities into the shared IR — the final plan mixes
  implementations from three sources: engine base, engine optimizations, Gluten library.
* **Three-way precedence**: a function may have Velox, Bolt, and Gluten implementations.
  Registration order means the Gluten overlay wins — correct for Spark-semantics fixes, but it
  could shadow a faster Bolt implementation. The per-entry config gating plus the init-time
  manifest (Section 7) makes every contested name visible and switchable rather than silent.

The resulting topology:

```
┌─ IR contract half — engine-neutral, shared ──────────────────────────────────┐
│ gluten-substrait: detail protos · RelBuilder.makeExtensionRel ·              │
│ transformer conventions · per-entry config gating                            │
└──────────────────────────────────────────────────────────────────────────────┘
            │                          │                          │
        same type URL on the wire, per-backend registration
            │                          │                          │
            ▼                          ▼                          ▼
┌────────────────────────┐ ┌────────────────────────┐ ┌────────────────────────┐
│ backends-velox +       │ │ backends-bolt          │ │ backends-ch            │
│ cpp/velox              │ │ (#12454 / #12456)      │ │                        │
│                        │ │                        │ │ IR half available,     │
│ rel handlers,          │ │ same C++ if APIs       │ │ adoption is            │
│ operators,             │ │ hold, else port;       │ │ opt-in                 │
│ function overlay       │ │ Bolt-only rels too     │ │                        │
└────────────────────────┘ └────────────────────────┘ └────────────────────────┘

A backend with no handler for a type URL fails validation for that rel and
the planner falls back — the IR contract holds even where code doesn't port.
```

One caveat to track: engine-internal optimizations like Bolt's operator fusion may not recognize
Gluten custom plan nodes, so overlay operators can act as fusion barriers there. This is an
argument for keeping overlay operators coarse and self-contained, and for mapping an overlay type
URL onto a Bolt-native feature where one exists instead of recompiling the Gluten implementation.

Memory is the other integration axis: Bolt advertises its own native memory management with
dynamic off-heap thresholds. It must plug into Gluten's accounting spine as an engine adapter
(Section 10.2) rather than act as a second budget authority beside Spark's TaskMemoryManager.

## 10. Memory management

Mixed plans only work if every operator in the stage participates in one memory-management
regime. Today that regime has three roles, split as follows:

* **Accounting / reservation — already Gluten-owned and engine-neutral.**
  `AllocationListener` (`cpp/core/memory/AllocationListener.h`) bridges every native allocation
  to Spark's `TaskMemoryManager` via JNI. Engine pools do the byte-tracking; the budget authority
  is Spark, through Gluten.
* **Arbitration entry points — Gluten-owned shim, thin policy.** `ListenableArbitrator`
  (`cpp/velox/memory/VeloxMemoryManager.cc`) implements `velox::memory::MemoryArbitrator`:
  grow = reserve more from Spark via the listener; shrink (Spark demanding memory back) =
  `pool->reclaim(target)`, delegating everything else to Velox.
* **Reclamation — entirely the engine's.** Victim selection and spill mechanics
  (`MemoryReclaimer`, spill files, `SpillConfig`) happen inside Velox's pool tree.

In one picture:

```
        JVM                                native (per executor)
┌──────────────────────────┐            ┌─ accounting (cpp/core) ──────────────────────────┐
│ Spark TaskMemoryManager  │   JNI      │ AllocationListener — the engine-neutral spine;   │
│ = the budget authority   │ ◄────────► │ every native byte is reserved from Spark         │
└──────────────────────────┘ reserve /  └──────────────────────────────────────────────────┘
                            free               ▲ grow                │ shrink
                                               │                     ▼
                            ┌─ arbitration policy (cpp/velox) ─────────────────────────┐
                            │ ListenableArbitrator (velox::memory::MemoryArbitrator):  │
                            │ grow   = reserve more from Spark via the listener        │
                            │ shrink = pool->reclaim(target) — delegate to the engine  │
                            └──────────────────────────────────────────────────────────┘
                                                          │
                                                          ▼
                            ┌─ reclamation mechanics (engine-owned) ───────────────────┐
                            │ Velox memory pool tree: query ─ task ─ node ─ operator   │
                            │ MemoryReclaimer → spill framework (SpillConfig)          │
                            │                                                          │
                            │ engine ops and overlay ops draw from the same tree;      │
                            │ a buffering overlay op MUST implement reclaim() (§10.1)  │
                            └──────────────────────────────────────────────────────────┘
```

Because overlay operators are real Velox operators drawing from Velox pools, *accounting* for
mixed plans costs nothing extra. The gap is *reclaim participation*: none of Gluten's existing
custom operators implements a reclaimer, so a buffering overlay operator would be invisible to
spill — under memory pressure the arbitrator cannot reclaim from it and the stage OOMs instead
of spilling. The overlay must close this gap by contract, and the design deliberately does
**not** replace Velox's in-stage pool-tree/reclaimer mechanics (the part Bolt also inherited);
Gluten owns the budget, the policy, and the contract, and delegates the mechanics.

### 10.1 Memory contract for overlay operators (required)

Every overlay operator entry must:

1. Allocate only from its operator `pool()` — never `malloc`/Arrow pools inside the operator,
   which escape both accounting and reclaim.
2. Declare whether it buffers (holds more than the current input batch).
3. If it buffers, implement `Operator::reclaim()` using Velox's spill framework, taking
   `SpillConfig` from `DriverCtx` so it inherits the spill directories, compression, and
   thresholds Gluten already wires through QueryConfig. Non-spillable buffering must be bounded
   and declared (`canReclaim() == false`), and is grounds for extra review scrutiny.
4. Pass a **memory-pressure test**: a shared harness runs the operator under an artificially
   small capacity, forces arbitration, and asserts it spills or fails gracefully rather than
   OOMs.

This is a per-entry review-checklist item with the same weight as native validation support.

### 10.2 Engine-neutral memory bridge (needed for Bolt regardless)

The engine-neutral spine (listener, reservation blocks, dynamic off-heap sizing) already lives in
`cpp/core`; the Velox-specific piece is only the `ListenableArbitrator` shim. Make the contract
explicit — *an engine adapter implements grow-via-listener and shrink-via-engine-reclaim* — and
require each backend to provide its adapter. `backends-bolt` in particular must route Bolt's own
memory management through this adapter: two competing budget authorities (Bolt thresholds vs.
Spark's TaskMemoryManager) in one executor produce untraceable OOMs. One accounting spine,
per-engine reclaim adapters. This belongs in the #12456 common-layer extraction alongside the
overlay's IR half.

### 10.3 Gluten-owned arbitration policy (improvement opportunity)

Today, on shrink, Gluten hands the engine a byte target and Velox picks victims by its own pool
traversal. Gluten has context the engine lacks and can own the policy by growing
`ListenableArbitrator` from a delegating shim into a policy layer that walks the pool tree and
issues targeted `reclaim()` calls:

* Reclaim in cheapness order: free/unused reservation first (partially done), then
  `AsyncDataCache`/SSD cache eviction, then operator spill — preferring cheap spills (sort)
  over expensive ones (hash build mid-probe).
* Coordinate across the executor's tasks rather than per-pool.
* Coordinate on-heap/off-heap elasticity with Spark's dynamic off-heap sizing.

### 10.4 Observability

Extend the manifest (Section 7) to memory: per plan, expose which operators are reclaimable,
arbitration/spill events, and peak pool usage per operator into task stats and the Spark UI. In a
mixed Velox+Bolt+Gluten plan, "who held the memory and who refused to spill" must be answerable
from metrics.

## 11. Non-goals

* Not a fork of Velox/Bolt: the `staging` lifecycle and the review bar in Section 6 exist to keep
  the overlay small.
* Not a new IR: everything rides on standard Substrait extension points already vendored in
  Gluten.
* Not (yet) an override mechanism for built-in engine operators — designed for (Section 5.5) but
  deferred until a concrete candidate exists.
* No change to the CH backend: the IR half is available to it, adoption is opt-in.
* No Gluten memory manager inside the stage: Velox's pool-tree/reclaimer mechanics stay; Gluten
  owns budget, policy, and contract (Section 10).

## 12. Implementation plan

1. **Land #12817** (function overlay) — done/in review; the operator overlay mirrors its shape.
2. **Operator overlay infrastructure + GlutenStride** — rework #12739: `ExtensionSingleRel`
   encoding, rel-handler registry, `registerOperatorOverlay()`, `RelBuilder.makeExtensionRel`,
   overlay README with lifecycle policy and the memory contract (Section 10.1). Stride remains
   the minimal mechanism demo.
3. **GlutenSample** — first rule-driven, stage-embedded, config-gated library entry; replaces the
   filter emulation behind `spark.gluten.sql.overlay.sample.enabled`. Ships together with the
   shared memory-pressure test harness.
4. **Manifest + docs** — init-time manifest logging; extend
   [velox-function-development-guide.md](./velox-function-development-guide.md) with the operator
   guide; land [velox-api-usage.md](./velox-api-usage.md) (#12773) as the extension-point
   contract / Bolt compatibility checklist.
5. **(With #12456)** move the IR half and the memory-bridge contract (Section 10.2) into the
   extracted common layer so `backends-bolt` shares them; run the compatibility checklist
   against Bolt.
6. **(Later, on demand)** post-conversion rewrite registry for overriding built-in engine
   operators; Gluten-owned arbitration policy (Section 10.3); consolidate pre-existing custom
   operators (`ValueStream`, `HashTableBuilder`, marker-based encodings like `isSMJ=`) onto the
   overlay conventions.
