# Design: Decoded Scan Cache for the Velox Backend

Scope: Velox backend, Parquet read path.

Status: phase 1 implemented, **not yet compiled or run**. Off by default
(`spark.gluten.sql.columnar.backend.velox.decodedCacheEnabled=false`). Phase 2 (the compact
L1 encoding and reading hits through `readWithVisitor`) is designed here but not written.

Phase-1 code:

| File | Contents |
|---|---|
| `cpp/velox/cache/DecodedCache.h/.cc` | key namespaces + decode-context identity, blob store over `AsyncDataCache`, ghost-list admission, split index, stats |
| `cpp/velox/cache/DecodedCacheReader.h/.cc` | wrapping `ReaderFactory`, `Reader`, and the serve/populate `RowReader` |
| `cpp/velox/tests/DecodedCacheTest.cc` | unit tests for the store, keys, serde, split index |
| `cpp/velox/compute/VeloxBackend.cc` | `initDecodedCache()`, factory registration, stats on shutdown |
| `cpp/velox/compute/VeloxPlanConverter.cc` | records `path -> (size, mtime)` while the mtime is still available |

## 1. Problem

Velox's `AsyncDataCache` (memory tier) + `SsdCache` (SSD tier) cache **raw file bytes**.
Gluten wires them up in `cpp/velox/compute/VeloxBackend.cc` (`initCache`, `initSsdCache`) and
hands the cache to every query through `QueryCtx` in
`cpp/velox/compute/WholeStageResultIterator.cc:280`. `GlutenBufferedInputBuilder`
(`cpp/velox/memory/GlutenBufferedInputBuilder.h`) picks `CachedBufferedInput` when a cache is
present and `GlutenDirectBufferedInput` otherwise.

That removes **IO** on a cache hit. It removes **no CPU**: every hit still pays

1. page decompression (snappy/zstd/gzip),
2. RLE / bit-pack / delta decoding,
3. dictionary page rebuild,
4. def/rep-level to null-bitmap reconstruction,
5. type coercion (int96 → timestamp, decimal rescale, date rebase).

On repeated interactive scans over a warm cache, that is where the scan time goes. It is also
the reason the SSD tier helps less than expected: an SSD hit still pays 1–5.

LiquidCache ([code](https://github.com/XiangpengHao/liquid-cache),
[VLDB'25 paper](https://www.vldb.org/pvldb/vol18/p5662-hao.pdf)) attacks exactly this: it
transcodes Parquet into a cache-only "Liquid" format on first read, keeps that in
memory/SSD, and evaluates filters directly on the encoded form. Reported: up to 10x lower
cache-side CPU time at no extra memory footprint.

This document proposes the equivalent for Gluten/Velox: a **decoded scan cache** keyed per
column chunk, holding a compact, filter-evaluable representation, sitting *below* filter and
mutation application so it is reusable across queries with different predicates.

## 2. What Velox already has (the honest delta)

Do not re-invent these — the design must reuse them:

- **Encoding-aware filter evaluation already exists.** `SelectiveColumnReader` carries a
  `ScanState` with `DictionaryValues` and a `filterCache`
  (`velox/dwio/common/SelectiveColumnReader.h`), so a filter on a dictionary-encoded column
  is evaluated once per distinct value, then applied over indices. The SIMD kernels live in
  `velox/dwio/common/ColumnVisitors.h` and `DecoderUtil.h`.
- **Lazy / selective materialization already exists** via `ScanSpec` filter ordering and
  `ColumnLoader`.
- **A tiered store with eviction, SSD spill and checkpointing already exists**:
  `AsyncDataCache` + `SsdCache`.

So the part of LiquidCache's advantage Gluten is missing is narrow but expensive: **the
decoded/transcoded bytes are thrown away after every read**. The design goal is therefore
*not* a new filter engine. It is: **persist a canonical, cheap-to-decode form of each column
chunk, and make Velox's existing visitor/filter machinery read from it.**

That reframing is what keeps this project tractable.

## 3. Goals / non-goals

Goals:

- Cut scan CPU on repeated reads of the same columns, in-process, per executor.
- Reuse across queries with **different predicates and different projections** — so the
  cached unit must be unfiltered and per-column.
- Coexist with the raw byte cache; never regress a cold, one-shot query.
- Land incrementally, with phase 1 requiring **no Velox patch**.

Non-goals (v1):

- Nested types (array/map/struct, flat maps) — fall back to the raw path.
- A disaggregated cache server (LiquidCache's distributed mode). This is a local, per-executor
  cache; cross-executor reuse depends on soft affinity, as the raw cache already does.
- Caching write paths or shuffle.

## 4. Cache unit and key

**Unit:** one *chunk* = (leaf column, row-group, row range), unfiltered, unpruned, all rows
present (nulls as a bitmap). Row range exists because `SsdFile` caps a single entry at
`1 << SsdFile::kSizeBits` = **8 MB** (`velox/common/caching/SsdFile.h:41,49`); a wide column
chunk must be split into several entries. Default target: 64 Ki rows per chunk, hard-split at
8 MB.

Choosing *leaf column × row group* rather than "the RowVector a split produced" is the whole
point: a later query projecting a different column set or applying a different filter still
hits on the columns it shares.

**Key:** reuse `cache::RawFileCacheKey{fileNum, offset}` so the existing store can be used
unchanged.

- `fileNum = fileIds().lease(keyString).id()` with
  `keyString = "gluten-decoded/v" + kFormatVersion + "/" + filePath + "@" + mtime + ":" + fileSize + "/" + columnPath + "/" + decodeContextHash`
- `offset = rowGroupOrdinal * kChunksPerRowGroupMax + chunkOrdinal` (a synthetic ordinal, not
  a file offset).

Sourcing the identity fields inside the overlay needs care:

- `filePath` — `BufferedInput::getName()` (delegates to `ReadFile::getName()`), or
  `ReaderOptions::fileHandle()->file->getName()`.
- `fileSize` — `ReadFile::size()`, already known, no extra request.
- `mtime` — **not reachable from the reader today.** Gluten does plumb it into splits
  (`cpp/velox/compute/VeloxPlanConverter.cc:202` sets `FileProperties::modificationTime`), and
  `FileSplitReader::createReader` passes `FileProperties` to `fileHandleFactory_->generate`,
  but `FileHandle` itself keeps only `{file, uuid, groupId}` — the mtime is dropped before the
  reader sees it. Overlay workaround: Gluten builds splits in the same executor process, so it
  can record `path -> (mtime, size)` in a small process-wide map at split-creation time and
  consult it in the factory. Falling back to size-only is possible but weaker: a rewritten
  file of identical length would serve stale decoded chunks.

No `HeadObject`/`getFileStatus` is added on any of these paths.

### 4.1 `decodeContextHash` — the main new correctness hazard

Raw bytes are self-describing; **decoded values are not**. Two queries can legitimately decode
the same Parquet bytes to different values. The hash must cover at least:

- target Velox type for the column (schema evolution / requested-type coercion),
- timestamp unit and `int96AsTimestamp` handling,
- date/timestamp calendar rebase mode (`spark.sql.parquet.datetimeRebaseModeInRead`,
  `int96RebaseModeInRead`),
- decimal precision/scale target,
- `binaryAsString`, session time zone,
- case sensitivity of the column resolution.

Anything session-dependent that changes decoded values and is *not* in this hash is a
silent-wrong-results bug. Guardrail: build the hash from an explicit allowlist struct, and add
a unit test that fails when a new field is added to the reader options struct without being
added to the hash.

Pruning is kept out of the key by always caching the **full leaf column** — never a
subfield-pruned projection.

## 5. Where it hooks into the read path

**The whole feature is implementable as a Gluten overlay — no Velox patch is required.** See
§11 for the verified extension surface and its churn exposure. Three candidate insertion
points:

| # | Point | Velox patch? | Reuses Velox filter kernels | Reuse across queries |
|---|---|---|---|---|
| A | Wrap `FileSplitReader::next()` output | no | n/a | poor (filter+projection in key) |
| B | Wrapping `ReaderFactory` for `FileFormat::PARQUET` | no | partially | good |
| C | Gluten-private `FormatParams`/`FormatData` + leaf readers, behind B | no | fully | good |

**Reject A.** Caching produced `RowVector`s forces filter signature and projection into the
key; a warm cache would miss on any predicate change. This is the trap to avoid.

**Phase 1 = B.** Gluten already owns the registration site: `VeloxBackend.cc:260` calls
`velox::parquet::registerParquetReaderFactory()`. Replace it with a Gluten factory for
`FileFormat::PARQUET` that delegates to `ParquetReaderFactory` and wraps the returned
`Reader`/`RowReader`. The wrapper:

- forwards all metadata calls (`rowType()`, `numberOfRows()`, `fileMetaData()`, row-group
  pruning) to the real `ParquetReader`, so stats-based row-group skipping is untouched;
- on `createRowReader`, inspects `RowReaderOptions::scanSpec()`
  (`velox/dwio/common/Options.h:383`) to classify each leaf column as *filter column* or
  *projection-only*, and to decide cacheability;
- per row group, for each cacheable leaf column, looks the chunk up in the decoded cache.

Registering at the factory rather than subclassing split readers is what makes one
implementation cover Hive, Iceberg and Delta: all of them reach the reader through
`dwio::common::getReaderFactory(fileFormat)->createReader(...)` in
`FileSplitReader::createReader` (`velox/connectors/hive/FileSplitReader.cpp:328`).
`FileSplitReader` is also virtual end-to-end and Gluten already subclasses `HiveSplitReader`
for Delta (`cpp/velox/compute/delta/DeltaSplitReader.h`), but that route has to be repeated
per connector.

**Phase 2 = C, still entirely in Gluten.** A cache hit is served by a Gluten-private
`FormatParams` / `FormatData` pair plus leaf readers subclassing the `Selective*ColumnReader`
family in `dwio/common`, with the cached chunk standing in for a `PageReader`. This is the
same extension surface Parquet, ORC, DWRF and Nimble each use, so filter evaluation runs
through the *unmodified* `readWithVisitor` path and inherits the SIMD kernels and
`filterCache`. In effect the decoded cache becomes a private file format whose backing store
is `AsyncDataCache` instead of a file.

**Optional, later: upstream the same idea as a first-class hook** — a
`dwio::common::ColumnChunkCache` on `RowReaderOptions`, consulted by `ParquetData`. That
deletes the filterless second pass of §5.1 and the wrapper layer. It is an ergonomics and
maintenance win, not a prerequisite.

### 5.1 The population problem

A `RowReader` returns rows with filters already applied, so on a miss the wrapper cannot
observe the unfiltered column. Options:

- **(a) Transcode pass.** For the row groups the query touches, read the *requested columns*
  with filters detached (a second `RowReaderOptions` with a filterless `ScanSpec` clone),
  transcode, admit, then serve the query from cache. First read of a selective query pays
  extra decode of rows it would have skipped — but only within row groups it already reads,
  and only once. This is what LiquidCache effectively does.
- **(b) Opportunistic.** Only populate when the read is already unfiltered. Zero first-query
  regression, much slower warm-up, and never warms the filter columns — which are the ones
  worth caching.
- **(c) Cache filtered results.** Rejected (see A).

**Recommendation: (a), gated by the admission policy in §7 so it only fires for chunks with
observed reuse.** Config `…decodedCache.populateOnFilteredRead` (default true) allows falling
back to (b) semantics. Note the read amplification of (a) is bounded by row-level selectivity
*within already-read row groups* — row-group and page-index skipping still apply, so a highly
selective point lookup does not turn into a full-table scan.

With phase-3 hook C the problem disappears: the leaf reader sees the chunk before filters.

### 5.2 Interaction with mutations

Iceberg positional deletes and Delta deletion vectors are applied as a `Mutation` at
`RowReader::next()` / in `DeltaSplitReader::next()`. The cache sits strictly *below* that, so
cached chunks are DV-independent and remain valid when a DV changes — as long as the DV is not
folded into the cached data. Do not "helpfully" pre-apply deletes at transcode time.

## 6. The cached representation

Three levels of ambition. Ship L1; L0 is the bring-up crutch, L2 is profile-driven.

- **L0 — serialized decoded vector.** Reuse the registered `PrestoVectorSerde` to serialize
  the decoded `VectorPtr`. This was the phase-1 bring-up representation and has been
  **replaced**: a borrowed wire format carries no magic or version of ours, so a stale blob
  would be misread rather than rejected; a hit cost a full window deserialize; and it did not
  compress. See `cpp/velox/cache/DecodedWindowFormat.h` for the format that replaced it —
  48-byte `WindowHeader` ('GLDW', version, type kind, encoding, offsets, min/max), then an
  optional null bitmap, then either plain values or a dictionary plus one narrow index per
  row. Index widths are byte-aligned (8/16/32) for now; the `indexBits` field accommodates
  true bit-packing without a format change.

- **L1 — canonical uncompressed dictionary page (recommended).** Store per chunk:
  - null bitmap (`bits::` layout, omitted if no nulls),
  - dictionary blob, in Velox `FlatVector` value layout (fixed-width values, or
    offsets + char blob for strings),
  - indices, bit-packed to `ceil(log2(dictSize))` bits,
  - or, when dictionary encoding does not pay (high cardinality, or already compact
    fixed-width), the raw fixed-width value array — the "plain" fallback,
  - a small header: encoding kind, row count, dict size, index width, min/max, null count.

  The point of L1 is that this is *shape-compatible with what Velox already decodes*. Indices
  bit-packed at a fixed width are exactly what `RleBpDecoder`/`BitPackDecoder` consume; the
  plain fallback is what `DirectDecoder` consumes. So a hit can be fed to
  `SelectiveColumnReader::readWithVisitor` and inherit, for free: SIMD filter kernels,
  `DictionaryValues` + `filterCache` (one filter evaluation per distinct value), null-aware
  visitors, and `getValues` producing a `DictionaryVector` with the dictionary shared rather
  than copied. **No new filter engine is written.** Header min/max additionally allows whole-
  chunk skip before touching indices.

- **L2 — LiquidCache-style specialized encodings.** FSST for non-dictionary strings,
  frame-of-reference + delta for numerics and timestamps. These need new decode/filter kernels
  and only pay off on columns L1 compresses badly. Gate behind measurement; do not do this in
  v1.

Materialization on a hit depends on how the entry was allocated.
`AsyncDataCacheEntry::initialize(key, contiguous)` supports both layouts
(`velox/common/caching/AsyncDataCache.h`):

- non-contiguous (default): page runs, walked via `dataRanges(length)` — needs a copy into the
  query's pool to build a Velox `Buffer`;
- `contiguous = true`: a single region reachable through `contiguousData()`, plus a `tinyData_`
  inline path for entries under `kTinyDataSize` = 2048 bytes.

So v1 allocates chunk entries with `contiguous = true` and copies out (a memcpy, roughly an
order of magnitude cheaper than decompress + decode). Zero-copy is then a *later flip of the
same code path*, not a redesign: wrap `contiguousData()` in a `BufferView` whose releaser holds
the `CachePin`. Deferred rather than done in v1 because a pinned entry cannot be evicted, so a
long-lived `DictionaryVector` would hold cache capacity hostage — the pin lifetime needs its
own accounting before this is safe. `size_` is `int32_t`, so a single entry is bounded at 2 GB
independently of the 8 MB SSD limit from §4.

## 7. Storage, tiering, admission, eviction

**Reuse `AsyncDataCache` rather than building a second store.** The synthetic-`fileNum` key
space in §4 makes decoded chunks ordinary cache entries, which buys memory tier + SSD tier +
eviction + checkpoint/restore + `CachePin` concurrency (`findOrCreate` gives exclusive-mode
population with a `waitFuture`, so two threads racing on the same chunk transcode once) with
no new code. It also puts raw and decoded entries in **one budget**, competing by LRU — which
is the behaviour you want, since a decoded chunk makes its raw counterpart nearly worthless.

Constraint to respect: ≤8 MB per entry (§4).

**Admission — the most important knob.** Transcoding is only free when it is amortized; the
paper's own framing is that transcode cost hides behind IO. Spark ETL is full of
one-shot scans, so admit conservatively:

1. **Admit on second touch.** Keep a small per-executor ghost list (key → first-seen time,
   fixed capacity, e.g. 1M entries ≈ tens of MB) and only transcode a chunk whose key has been
   seen before. First scan behaves exactly like today; the second warms.
   `velox/common/caching/FileGroupStats.h` and `ScanTracker` already track per-column access
   and can seed this instead of a separate structure.
2. **Skip cheap columns.** If `transcodedBytes / rawBytes` exceeds a threshold (default 0.8),
   the column is already compact and decode-cheap (e.g. plain-encoded doubles) — the cache
   spends memory to save little. Drop it and let the raw cache serve it.
3. **Skip types not in the allowlist** and all nested types.

**Query-aware tiering ("squeeze").** LiquidCache's example — group by `year`, keep only `year`
resident and leave `timestamp` on disk — maps directly onto information Gluten already has in
`ScanSpec`: a column either carries a pushed filter or is projection-only. Policy: filter
columns are the memory tier's priority; projection-only chunks are marked with
`AsyncDataCache::makeEvictable(key)` after consumption so they drain to SSD first and the
memory tier fills with the chunks that predicates hit. `makeEvictable` already exists
(`velox/common/caching/AsyncDataCache.h`), so this is a policy change, not new machinery.

### 7.1 Standalone or shared store

The decoded cache needs *an* `AsyncDataCache` to hold windows; it does not need
the **raw byte cache**. Those are separable, so `decodedCacheEnabled` works either way:

- **`cacheEnabled` on** — share that instance. Decoded and raw entries then compete in one
  budget by LRU, which is the behaviour argued for above, and the SSD tier comes along.
- **`cacheEnabled` off** — build a private, memory-only store: a dedicated `MmapAllocator`
  sized by `decodedCacheMemSize` plus its own `AsyncDataCache`. This is the *decoded-only*
  configuration, and it is arguably the more sensible one: once decoded windows exist, the raw
  bytes of the same data are largely redundant.

A second `AsyncDataCache` instance is safe. `AsyncDataCache::create` only calls
`allocator->registerCache()` on the allocator it is given, and never touches the static
`AsyncDataCache::setInstance()`. That global is merely a default argument for `QueryCtx`, and
Gluten passes its cache explicitly (`WholeStageResultIterator.cc`), so it stays null.

Two constraints the private store must respect:

1. **Never hand it to `QueryCtx`.** Doing so would make `connectorQueryCtx_->cache()` non-null,
   which is what makes `FileSplitReader` choose `CachedBufferedInput` — the ordinary read path
   would start filling the decoded budget with raw bytes.
2. **Nothing in the reader may depend on the raw cache being on.** `FileSplitReader::createReader`
   only calls `setFileHandle()`/`setCache()` when a cache is present, so the file size for the
   cache key comes from `BufferedInput::getReadFile()->size()` rather than
   `ReaderOptions::fileHandle()`.

Like the raw cache arena, the private store's memory is **not tracked by Spark** — it has to be
budgeted in memory overhead. `VeloxListenerApi.onDriverStart` logs this when standalone mode is
selected, and recommends soft affinity in either mode.

**Invalidation** is by key: `mtime`+`size` in the key string means a rewritten file simply
misses. No explicit invalidation path, no stale-read window — conditional on actually having
`mtime`, see the sourcing caveat in §4. Cross-executor reuse relies on
soft affinity (`spark.gluten.soft-affinity.enabled`), same as the raw cache.

## 8. Config surface

`cpp/velox/config/VeloxConfig.h` + `backends-velox/src/main/scala/org/apache/gluten/config/VeloxConfig.scala`,
following the existing `…velox.ssdCache*` naming:

Implemented in phase 1:

| Key (`spark.gluten.sql.columnar.backend.velox.`) | Default | Meaning |
|---|---|---|
| `decodedCacheEnabled` | `false` | master switch; independent of `cacheEnabled` (see §7.1) |
| `decodedCacheMemSize` | `1GB` | size of the private store; used only when `cacheEnabled` is off |
| `decodedCacheWindowRows` | `65536` | rows per window; a window that serializes above 8 MB is not cached |
| `decodedCacheAdmitMinTouches` | `2` | ghost-list admission threshold; 1 warms on first touch |
| `decodedCacheServeFilteredReads` | `true` | when false, only filterless scans use the cache |
| `decodedCacheMaxPinnedBytesPerSplit` | `512MB` | a split is served only if all its windows pin within this |
| `decodedCacheMaxKeys` | `1048576` | hard bound on key namespaces (each holds a permanent `StringIdLease`) |

Designed but not implemented, deliberately left out until phase 2 has numbers to justify
them: `decodedCacheEncoding` (`plain`/`dictionary`/`liquid`), `decodedCacheMaxSizeRatio`
(needs per-chunk raw-size accounting from Parquet metadata), `decodedCacheTypes` (the
allowlist is currently hard-coded in `isCacheableType`), `decodedCacheMemoryRatio`.

## 9. Metrics

Without these the feature cannot be tuned, and there is a known gap here: Velox already
collects `overreadBytes` / `storageReadBytes` / `totalScanTime` / `numPrefetch` into Gluten's
`customStats`, but nothing promotes them to Spark SQL metrics. **Promoting scan stats to Spark
SQL metrics is a prerequisite, not a follow-up.** New counters:

- `decodedCacheHitBytes`, `decodedCacheHitChunks`, `decodedCacheMissChunks`
- `decodedCacheTranscodeWallNanos`, `decodedCacheTranscodedBytes`
- `decodedCacheAdmitRejected{Cheap,Ghost,Type}`
- `decodedCacheSsdReadBytes`, `decodedCacheEvictedBytes`
- `decodeCpuNanos` on the raw path, for the A/B comparison

## 10. Phasing

**Phase 0 — measurement (prerequisite).** Promote scan stats to Spark SQL metrics; add a
microbenchmark under `cpp/velox/benchmarks` that reads a warm-raw-cache Parquet file and
reports decode CPU by column type. This establishes the ceiling: if decode is not a large
fraction of warm scan CPU on the target workload, stop here.

**Phase 1 — L0 correctness skeleton, no Velox patch. Written; unverified.**
Gluten `ReaderFactory` layered over the registered Parquet factory; synthetic-key store on
`AsyncDataCache`; `PrestoVectorSerde` windows; flat primitives only; populate via §5.1(a);
ghost-list admission. Two things it still needs before it can be trusted:

- a build (see the status note at the top), and
- the differential test: every query run with the cache on and off must produce identical
  results, including nulls, timestamps under both rebase modes, decimals, schema evolution,
  and splits carrying deletion vectors. The unit tests cover the store, the key separation,
  the serde round-trip and the split-index framing; they do not cover the reader, which is
  where the risk actually is.

**Phase 2 — L1 encoding + filter reuse.** Canonical dict/bitpacked/plain chunk format; feed
hits through `readWithVisitor` so `ScanSpec` filters and `filterCache` apply to cached data;
`makeEvictable`-based query-aware tiering; SSD tier enabled and checkpoint-restore verified.
This is where the CPU win is expected to land.

**Phase 3 — L2 / nested types**, strictly profile-driven.

**Optional — upstream `dwio::common::ColumnChunkCache`** on `RowReaderOptions`, consulted by
`ParquetData`. Removes the filterless second pass and the wrapper layer, and generalizes to
ORC once the interface is format-neutral. Not on the critical path.

## 11. Overlay boundary and Velox-churn exposure

Everything above lives under `cpp/velox/` — no `ep/build-velox/src/*.patch` entry, no fork.
The Velox APIs it stands on, all verified against the pinned tree in
`ep/build-velox/build/velox_ep`:

| Need | API | Stability |
|---|---|---|
| take over Parquet reads | `registerReaderFactory` / `unregisterReaderFactory`, `ReaderFactory` (`velox/dwio/common/ReaderFactory.h`) | stable; documented plugin point |
| delegate to the real reader | `parquet::ParquetReaderFactory` — public default ctor (`ParquetReader.h:160`) | stable |
| file identity + cache handle | `ReaderOptions::fileHandle()` / `::cache()` (`Options.h:1015-1031`), set by `FileSplitReader::createReader` whenever `connectorQueryCtx_->cache()` is non-null | stable |
| file path / size | `BufferedInput::getName()`, `ReadFile::size()` | stable |
| filter + projection classification | `RowReaderOptions::scanSpec()` (`Options.h:383`), `ScanSpec::filter()` / `hasFilter()` | stable |
| filterless second pass | `BufferedInput::clone()`; `hive::makeScanSpec(...)` (`HiveConnectorUtil.h:85`) to build a fresh filterless spec | `makeScanSpec` signature does churn — it already carries a `@deprecated` overload |
| tiered store | `AsyncDataCache::findOrCreate` / `find` / `makeEvictable`, `CachePin`, `AsyncDataCacheEntry::contiguousData` / `dataRanges` | stable |
| serve hits (phase 2) | `FormatParams` / `FormatData` (`velox/dwio/common/FormatData.h`) + `Selective*ColumnReader` | **the churn-exposed layer** — internal, changes with reader refactors |

Two facts make this credible rather than optimistic. First, Gluten already carries overlays of
exactly this kind: `GlutenBufferedInputBuilder` is registered through Velox's own
`BufferedInputBuilder::registerBuilder` hook, `GlutenDirectBufferedInput` is a 98-line subclass
of a `dwio/common` internal, and `DeltaSplitReader` subclasses `HiveSplitReader`. Second, the
`FormatParams`/`FormatData`/`Selective*ColumnReader` triple is not a private backdoor — it is
the seam Parquet, ORC, DWRF and Nimble each sit on, so an out-of-tree format reader is a
supported shape.

The honest cost: phase 2 lands on the one layer that moves. Gluten already absorbs this kind
of drift with `#if __has_include(...)` shims — see `cpp/velox/compute/delta/DeltaSplitReader.h`,
which straddles the `SplitReader` → `FileSplitReader` rename. Budget for the same here, and
keep the phase-2 leaf readers thin so a Velox-side signature change is a mechanical fix.

Three things that are *not* overlay-solvable and must be accepted or worked around:

1. **mtime is dropped before the reader** (§4) — needs a Gluten-side path→mtime map.
2. **The filterless second pass** (§5.1) exists only because a `RowReader` will not hand out
   pre-filter columns. This is the concrete cost of staying out-of-tree.
3. **`createFormatOptions` must be forwarded.** `ReaderFactory::createFormatOptions` is a real
   override on `ParquetReaderFactory`; a wrapper that forgets to delegate it silently drops all
   Parquet session/connector options. Easy to miss, no compile error, subtle behaviour change.

## 12. Risks

- **Silent wrong results from an incomplete `decodeContextHash`** (§4.1). Highest-severity
  risk in the design; mitigated by allowlist struct + a test that breaks on new option fields.
- **Memory competition.** Decoded chunks are larger than the compressed bytes they replace;
  with one shared budget an aggressive decoded cache can push out raw entries that were
  serving a different workload. `decodedCacheMemoryRatio` plus admission rule 2 bound this,
  but it needs measurement under memory pressure, not just on an empty cache.
- **Transcode cost on one-shot scans.** The reason admission defaults to second-touch.
- **Velox rebase churn.** Gluten tracks Velox daily. The phase-1 factory sits on stable APIs;
  the phase-2 leaf readers sit on internal ones. See §11.
- **Win may be masked on IO-bound workloads.** On object storage the raw cache already removes
  the dominant cost on a second run; benchmark with the raw cache warm so the comparison
  isolates CPU.
- **Phase-1-specific, from writing it.** Known limitations of the code as it stands, each a
  deliberate trade rather than an oversight:
  - `DecodedCache` serializes key lookup, admission and file registration behind one mutex,
    hit once per column per split on the scan path. Short critical section, but global.
    Shard it if profiling shows contention.
  - The L0 window is a `PrestoVectorSerde` blob, so it does not compress and a hit costs a
    full deserialize of the window even when the caller wants a few rows. This is why L0 is
    a correctness skeleton and not the deliverable; L1 is where the CPU win is expected.
  - The populating pass reads unfiltered and applies the query's filters afterwards, so the
    query that warms a split loses selective-read benefits for that one run.
  - Eligibility bails out entirely on: nested types, synthesized row-number columns,
    `ColumnSelector`-based reads, `randomSkip`, `skipRows`, and `deltaUpdate` specs. Each
    falls back to the untouched Velox path, so the effect is lost caching, never wrong rows.
  - The split index is written only by a pass that reaches end of split, so an
    early-terminated query (a LIMIT) warms windows but never enables serving for that split.
