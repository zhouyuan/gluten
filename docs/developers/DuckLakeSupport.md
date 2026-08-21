---
layout: page
title: "[DRAFT] DuckLake Support in Gluten"
nav_order: 100
parent: Developer Overview
---

# [DRAFT] Proposal: DuckLake Support in Gluten

Status: **Draft for discussion** — not yet implemented.

## 1. Background

[DuckLake](https://github.com/duckdb/ducklake) is an open lakehouse format from the DuckDB
team. Like Iceberg/Delta/Hudi (which Gluten already supports), the table data is plain
Parquet on object storage. The key difference is where metadata lives:

| | Iceberg | Delta | Hudi | DuckLake |
|---|---|---|---|---|
| Metadata | Avro/JSON manifest files | JSON/Parquet `_delta_log` | Timeline files | **SQL tables in a catalog database** (DuckDB / PostgreSQL / MySQL / SQLite) |
| Data | Parquet/ORC/Avro | Parquet | Parquet | Parquet |
| Deletes | Position/equality delete files, Puffin DVs (v3) | Roaring-bitmap deletion vectors | MoR log files | **Parquet positional delete files** (`file_path`, `pos`) — Iceberg-style; Puffin DVs experimental |
| Update strategy | CoW or MoR | CoW or MoR (DV) | CoW or MoR | MoR (`UPDATE` = `DELETE` + `INSERT`) |
| Extras | — | CDF, column mapping | — | Data inlining (small data stored in catalog tables), per-file encryption, snapshots/time travel, CDF |

DuckLake v1.0 defines ~28 catalog tables (`ducklake_snapshot`, `ducklake_table`,
`ducklake_column`, `ducklake_data_file`, `ducklake_delete_file`,
`ducklake_file_column_stats`, `ducklake_partition_info`, ...). Planning a scan means
running SQL against the catalog DB: resolve the snapshot, resolve the column set valid at
that snapshot, list active data files and their associated delete files.

For the native engine this is friendly territory: **the scan itself is just
"Parquet files + optional positional delete files"**, which is a subset of what the Velox
Iceberg connector already handles.

## 2. Why this fits Gluten well

- Data files are vanilla Parquet — Velox/ClickHouse parquet readers apply directly.
- DuckLake positional delete files use the Iceberg positional-delete schema
  (`file_path`, `pos`). Gluten already ships the full wiring for that:
  `IcebergReadOptions.DeleteFile` in the Substrait proto → `IcebergSplitInfo` →
  Velox's `HiveIcebergSplit`/Iceberg connector. Most of the native side can be reused
  rather than rebuilt.
- All metadata resolution happens on the Spark **driver** at planning time (SQL queries
  against the catalog DB); executors and the native engine never need to talk to the
  catalog. This matches Gluten's existing split-planning model exactly.

## 3. The gap: no Spark read path exists yet

Unlike Iceberg/Delta/Hudi, there is **no upstream Spark DSv2 reader for DuckLake today**
(as of 2026-08). The community connector
[motherduckdb/ducklake-spark](https://github.com/motherduckdb/ducklake-spark) is a DSv2
`TableCatalog` + write-only connector (append mode, Spark 3.x, no reads). Gluten's
existing lakehouse integrations all work by *offloading an existing Spark scan node*
(`BatchScanExec` / `FileSourceScanExec`), so we must choose:

**Option A (recommended): depend on / contribute to `ducklake-spark` for the JVM read
path, then offload it.**
- Help land a `ScanBuilder`/`Batch` read implementation in `ducklake-spark` (planning via
  DuckDB JDBC + `ducklake` extension, or plain JDBC SQL against the catalog DB — the spec
  is just SQL, so PostgreSQL/MySQL/SQLite catalogs need no DuckDB at all).
- Gluten then adds `OffloadDuckLakeScan` matching that connector's `Scan` class, exactly
  like `OffloadIcebergScan` matches `SparkBatchQueryScan`.
- Pros: clean separation (format logic upstream, offload logic in Gluten), vanilla-Spark
  fallback comes for free (the JVM connector is itself the fallback).
- Cons: external dependency on a young project; needs Spark 3.5/4.0 support upstream.

**Option B: implement a minimal DSv2 reader inside `gluten-ducklake` itself.**
- Gluten owns catalog access + split planning end to end.
- Cons: without a JVM row-based reader, *fallback is impossible* — any unsupported
  feature becomes a query failure instead of a graceful fallback. This breaks Gluten's
  fallback contract and duplicates work that belongs upstream.

This draft assumes **Option A**, with Option B only as a stop-gap for a PoC.

## 4. Proposed module layout

Follow the Paimon module (newest, cleanest precedent) and the Hudi module (minimal
read-only precedent):

```
gluten-ducklake/                                  # new Maven module, -Pducklake profile
  src/main/scala/org/apache/gluten/
    execution/DuckLakeScanTransformer.scala       # extends BatchScanExecTransformerBase
    execution/OffloadDuckLakeScan.scala           # OffloadSingleNode: BatchScanExec -> transformer
    config/GlutenDuckLakeConfig.scala             # spark.gluten.sql.columnar.ducklake.*
  src/main/java/org/apache/gluten/substrait/rel/
    DuckLakeLocalFilesNode.java                   # only if we don't reuse Iceberg wire format

backends-velox/src-ducklake/                      # added via build-helper in -Pducklake
  main/scala/org/apache/gluten/component/VeloxDuckLakeComponent.scala
  main/resources/META-INF/gluten-components/org.apache.gluten.component.VeloxDuckLakeComponent
```

`VeloxDuckLakeComponent` declares `dependencies() = classOf[VeloxBackend] :: Nil`, an
`isRuntimeCompatible` reflection check for the DuckLake Spark connector class, and
injects `OffloadDuckLakeScan` via `HeuristicTransform.Simple(Validators.newValidator(...))`
— same pattern as `VeloxIcebergComponent` / `VeloxPaimonComponent`.

Root `pom.xml` gains a `-Pducklake` profile (module + versioned source roots), mirroring
`-Ppaimon`.

## 5. Read path design

### Phase 1 — append-only tables (no deletes, no inlined data): **zero C++ changes**

`DuckLakeScanTransformer.getSplitInfosFromPartitions` unpacks the connector's input
partitions (each carrying data-file path / start / length / partition values, resolved on
the driver from `ducklake_data_file`) into the generic `LocalFilesNode`. The native side
sees a plain Hive/Parquet scan — identical to how Hudi COW works today.

Validation (`doValidateInternal`) falls back when:
- any live `ducklake_delete_file` exists for the scanned files (Phase 1 only),
- the snapshot has inlined data (`ducklake_inlined_data_tables`),
- any file has `encryption_key` set,
- column types unsupported by the backend (UUID, etc. — same list as Iceberg).

### Phase 2 — merge-on-read deletes: **reuse the Iceberg delete-file wire format**

DuckLake's Parquet delete files are Iceberg-style positional deletes. Two sub-options:

- **2a (preferred): reuse `IcebergReadOptions`.** Emit each DuckLake data file as a
  `FileOrFiles` with `IcebergReadOptions` whose `DeleteFile` entries have content =
  `POSITION`. Native side needs at most a small guard (DuckLake delete files carry no
  Iceberg sequence numbers; `partial_max`/snapshot-embedded partial delete files must be
  validated against on the JVM side and fall back for now). No new proto, no new
  connector, no new `SplitInfo` subclass.
- **2b: dedicated `DuckLakeReadOptions`** in
  `gluten-substrait/src/main/resources/substrait/proto/substrait/algebra.proto` (new
  entry in the `file_format` oneof, documented in `SubstraitModifications.md`), plus
  `cpp/velox/compute/ducklake/DuckLakePlanConverter` producing a `SplitInfo` subclass.
  Only needed if 2a's semantics diverge (e.g. we later push snapshot-aware partial
  delete filtering or DuckLake encryption to native).

Start with 2a; move to 2b only when a concrete feature forces it.

### Phase 3+ — later

- **Data inlining**: inlined rows live in catalog SQL tables, invisible to native. Either
  fall back the whole scan (simplest), or have the JVM connector materialize inlined rows
  and Gluten union them via a small JVM-side `ColumnarBatch` source. Start with fallback.
- **Encryption**: per-file `encryption_key`; fall back until Velox parquet reader
  supports the scheme.
- **Puffin deletion vectors** (`ducklake_write_deletion_vectors`, experimental): fall
  back, same as Gluten does for Iceberg v3 puffin today.
- **Time travel / CDF**: time travel is free if the JVM connector resolves the snapshot
  before planning (Gluten just sees a file list). CDF follows the Delta CDF precedent
  (`DeltaCDFScanRule`) later.
- **Native write**: out of scope for the initial proposal. When tackled, follow the
  Iceberg native-write shape: Velox writes Parquet + returns file metrics as JSON, JVM
  commits by inserting into `ducklake_data_file`/`ducklake_snapshot` (or via the
  `ducklake_add_data_files`-style flow the MotherDuck connector uses). Column stats
  (`ducklake_file_column_stats`) must be produced at write time for correct pruning.

## 6. Fallback / support matrix (initial target)

| Feature | Initial support |
|---|---|
| Plain Parquet scan (append-only) | Offload |
| Filter/column pruning (via `ducklake_file_column_stats`) | Driver-side pruning, offload |
| Positional delete files (parquet) | Offload (Phase 2) |
| Partial delete files (`partial_max`) | Fallback |
| Puffin DVs | Fallback |
| Inlined data | Fallback |
| Encrypted files | Fallback |
| Schema evolution (column mapping via ducklake column ids ↔ parquet field ids) | Offload where field-id mapping suffices (reuse Iceberg `fieldId` handling); fallback otherwise |
| Time travel (`AT SNAPSHOT/TIMESTAMP`) | Offload (resolved at planning) |
| CDF | Fallback |
| Writes / DDL | Fallback (JVM connector) |

Config surface:

```
spark.gluten.sql.columnar.ducklake.enabled            (default: true when -Pducklake built)
spark.gluten.sql.columnar.ducklake.enableNativeRead   (default: true)
```

## 7. Open questions

1. **Upstream vs in-repo reader** — confirm Option A with the community; gauge
   MotherDuck's interest in a read path in `ducklake-spark` (tracked upstream in
   duckdb/ducklake#78 and #154).
2. **Catalog access** — require DuckDB JDBC + `ducklake` extension on the driver, or
   speak plain SQL to PostgreSQL/MySQL/SQLite catalogs directly? The spec is pure SQL,
   so direct JDBC avoids a native DuckDB dependency on the driver.
3. **Spec version pinning** — DuckLake is young (v1.0); decide which spec versions the
   validator accepts before offloading.
4. **ClickHouse backend** — this draft targets Velox first; CH backend can follow the
   same JVM module with its own component.

## 8. Milestones

1. `-Pducklake` profile + module skeleton + `VeloxDuckLakeComponent` + docs page.
2. Phase 1 read offload (append-only) + UTs against a SQLite/DuckDB catalog fixture.
3. Phase 2 positional deletes via `IcebergReadOptions` reuse + delete-file UTs.
4. Feature-matrix doc (`docs/get-started/VeloxDuckLake.md`) + CI job.
5. Phase 3 items driven by demand.
