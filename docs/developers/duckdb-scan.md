---
layout: page
title: DuckDB Scan Offload
nav_order: 21
parent: Developer Overview
---

# DuckDB Scan Offload (Experimental)

Gluten can offload plain parquet table scans to [DuckDB](https://github.com/duckdb/duckdb)
while all downstream operators keep running on the Velox backend. It follows the same
architecture as the DataFusion scan offload: Substrait stays the IR handed to the engine — here
consumed by DuckDB's [substrait extension](https://github.com/substrait-io/duckdb-substrait-extension)
(`from_substrait`) — and the scan runs as its own leaf stage whose Arrow output feeds the Velox
pipeline through Gluten's existing Arrow-native batch convention.

## Architecture

```
Spark FileSourceScanExec (parquet)
  └─ DuckDBScanExec (leaf, Arrow-native batches, whole-file splits)
       │ per task: describeParquet (physical column order)
       │           → standard substrait Plan (ReadRel + local_files + positional projection)
       ▼ JNI
     libgluten_duckdb (C++: DuckDB from_substrait → parquet_scan)
       │ Arrow C Data Interface (zero-copy)
       ▼
  ArrowColumnarToVeloxColumnarExec (existing transition)
  └─ InputIteratorTransformer → WholeStageTransformer (Velox: filter/agg/join/…)
```

Components:

- `cpp/duckdb`: a small C++ JNI shim (no protobuf dependency) that executes a self-contained
  Substrait plan through DuckDB's `from_substrait` and streams record batches back over the
  Arrow C Data Interface. DuckDB is built from source with the parquet extension statically
  linked; the substrait extension is loaded at runtime.
- `backends-velox/src-duckdb`: `VeloxDuckDBComponent` (a Gluten `Component` depending on
  `VeloxBackend`) injects `OffloadDuckDBScan`, which replaces supported `FileSourceScanExec`s
  with `DuckDBScanExec`. Unsupported scans keep going down the regular Velox scan path.

Because DuckDB's substrait consumer resolves the plan positionally against the parquet file's
physical schema, the per-task plan is assembled on the executor: the task first asks the native
side for the file's physical column order (`describeParquet`), computes the positional
projection, and only then builds the final Substrait plan (`DuckDBSubstraitPlanBuilder`).

## Build

```bash
./dev/builddeps-veloxbe.sh --enable_duckdb=ON
mvn clean install -Pbackends-velox -Pspark-3.5 -Pduckdb -DskipTests
```

The CMake build fetches and compiles DuckDB (pinned in `cpp/duckdb/CMakeLists.txt`) and drops
`libgluten_duckdb` into `cpp/build/releases/`, from where the backends-velox jar packaging picks
it up like the other native libraries.

The substrait extension is not statically linked yet (building it needs vcpkg-provided protobuf
packages); by default it is installed from the DuckDB community repository on first use, which
needs network access once per host. Air-gapped clusters can point
`spark.gluten.sql.columnar.duckdb.substraitExtensionPath` at a locally built extension instead.

## Usage

```
spark.gluten.sql.columnar.duckdb.scan.enabled=true
```

Additional configs:

| Config | Default | Description |
|---|---|---|
| `spark.gluten.sql.columnar.duckdb.scan.enabled` | false | Offload plain parquet file scans to DuckDB. |
| `spark.gluten.sql.columnar.duckdb.scan.timestampEnabled` | false | Allow timestamp columns (pending timezone-semantics validation). |
| `spark.gluten.sql.columnar.duckdb.threads` | 1 | Threads of each scan's DuckDB instance (one instance per Spark task; 0 = DuckDB default). |
| `spark.gluten.sql.columnar.duckdb.memoryLimit` | (DuckDB default) | DuckDB memory_limit per scan instance, e.g. `1GB`. |
| `spark.gluten.sql.columnar.duckdb.substraitExtensionPath` | (community) | Path of a locally built substrait extension on every node. |

## Stage-1 scope and fallbacks

A scan is offloaded only when all of the following hold; otherwise it falls back to the Velox
scan transparently:

- Parquet `FileSourceScanExec` on a local (`file:`) filesystem.
- No partition columns in the scan output: DuckDB's substrait consumer cannot synthesize Hive
  partition values. Reading only data columns of a partitioned table is fine.
- No bucketing, metadata columns (`_metadata`, `input_file_name`, row-index), or parquet field
  IDs.
- Flat output types: boolean, integral, float/double, decimal, string, binary, date (timestamp
  behind the config above).
- Filters are not pushed into the DuckDB scan; they stay as Velox `FilterRel`s above it, so
  results remain correct without translating Gluten's expression dialect yet.

DuckDB-specific behavior:

- DuckDB's `parquet_scan` cannot read a byte range of a file, so `DuckDBScanExec` plans splits of
  whole files (a file larger than `spark.sql.files.maxPartitionBytes` makes one bigger task
  instead of several ranged ones).
- All files of a split must share one physical parquet schema (they normally do, being written by
  one job); `parquet_scan` over a file list requires it anyway.

Known stage-1 gaps (by design, to be addressed in follow-ups):

- DuckDB's memory is not accounted to Spark's memory manager (bound it per task with
  `memoryLimit`).
- One in-memory DuckDB instance per task; no shared instance or scan metrics beyond
  rows/batches/scan time.
- No S3/HDFS/ABFS object stores, complex types, filter/limit pushdown, or schema-evolution
  coercions (the batch types are the file's natural parquet types).
- The substrait extension is loaded at runtime rather than statically linked.

## Testing

```bash
# Native round-trip test (uses the extension's own get_substrait producer;
# skipped when the substrait extension cannot be installed).
./dev/builddeps-veloxbe.sh --enable_duckdb=ON --build_tests=ON
ctest --test-dir cpp/build -R gluten_duckdb_test

# Scala end-to-end suite (requires libgluten_duckdb to be built).
mvn test -Pbackends-velox -Pspark-3.5 -Pduckdb -pl backends-velox \
  -DwildcardSuites=org.apache.gluten.execution.DuckDBScanSuite
```
