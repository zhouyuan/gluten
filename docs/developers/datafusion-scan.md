---
layout: page
title: DataFusion Scan Offload
nav_order: 20
parent: Developer Overview
---

# DataFusion Scan Offload (Experimental)

Gluten can offload plain parquet table scans to [Apache DataFusion](https://github.com/apache/datafusion)
while all downstream operators keep running on the Velox backend. This is the first stage of a
broader DataFusion integration: Substrait stays the IR handed to DataFusion, and the scan runs as
its own leaf stage whose Arrow output feeds the Velox pipeline through Gluten's existing
Arrow-native batch convention.

## Architecture

```
Spark FileSourceScanExec (parquet)
  └─ DataFusionScanExec (leaf, Arrow-native batches)
       │ substrait Plan (one ReadRel) + per-task ReadRel.LocalFiles
       ▼ JNI
     libgluten_datafusion (Rust: prost-parsed ReadRel → DataFusion parquet scan)
       │ Arrow C Data Interface (zero-copy)
       ▼
  ArrowColumnarToVeloxColumnarExec (existing transition)
  └─ InputIteratorTransformer → WholeStageTransformer (Velox: filter/agg/join/…)
```

Components:

- `rust/gluten-datafusion`: a cdylib that compiles Gluten's patched Substrait protos with prost,
  maps the `ReadRel`/`LocalFiles` to a DataFusion `DataSourceExec` parquet scan and streams
  record batches back over the Arrow C Data Interface.
- `backends-velox/src-datafusion`: `VeloxDataFusionComponent` (a Gluten `Component` depending on
  `VeloxBackend`) injects `OffloadDataFusionScan`, which replaces supported
  `FileSourceScanExec`s with `DataFusionScanExec`. Unsupported scans keep going down the regular
  Velox scan path.

## Build

```bash
# Requires a Rust toolchain (rustup or Homebrew).
./dev/builddeps-veloxbe.sh --enable_datafusion=ON
mvn clean install -Pbackends-velox -Pspark-3.5 -Pdatafusion -DskipTests
```

The cargo build drops `libgluten_datafusion` into `cpp/build/releases/`, from where the
backends-velox jar packaging picks it up like the other native libraries. The library can also be
built standalone with `cargo build --release` under `rust/`.

## Usage

```
spark.gluten.sql.columnar.datafusion.scan.enabled=true
```

Additional configs:

| Config | Default | Description |
|---|---|---|
| `spark.gluten.sql.columnar.datafusion.scan.enabled` | false | Offload plain parquet file scans to DataFusion. |
| `spark.gluten.sql.columnar.datafusion.scan.timestampEnabled` | false | Allow timestamp columns (pending timezone-semantics validation). |
| `spark.gluten.sql.columnar.datafusion.threads` | 0 | Threads of the shared per-executor tokio runtime (0 = cores). |

## Stage-1 scope and fallbacks

A scan is offloaded only when all of the following hold; otherwise it falls back to the Velox
scan transparently:

- Parquet `FileSourceScanExec` on a local (`file:`) filesystem.
- No bucketing, metadata columns (`_metadata`, `input_file_name`, row-index), or parquet field
  IDs.
- Flat output types: boolean, integral, float/double, decimal, string, binary, date (timestamp
  behind the config above); partition column types limited to integral, string, date.
- Filters are not pushed into the DataFusion scan; they stay as Velox `FilterRel`s above it, so
  results remain correct without translating Gluten's expression dialect yet.

Known stage-1 gaps (by design, to be addressed in follow-ups):

- DataFusion's memory is not accounted to Spark's memory manager.
- No native scan metrics beyond rows/batches/scan time.
- No S3/HDFS/ABFS object stores, complex types, or filter/limit pushdown.

## Testing

```bash
# Rust unit + integration tests.
cd rust && cargo test

# Scala end-to-end suite (requires libgluten_datafusion to be built).
mvn test -Pbackends-velox -Pspark-3.5 -Pdatafusion -pl backends-velox \
  -DwildcardSuites=org.apache.gluten.execution.DataFusionScanSuite
```
