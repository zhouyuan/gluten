# Arrow JSON Reader Implementation

## Overview

This document describes the Arrow JSON reader implementation for Gluten, which follows the same pattern as the Arrow CSV reader. The implementation uses Apache Arrow's Dataset API to read JSON files efficiently.

## Architecture

The JSON reader implementation consists of the following components:

### Core Components

1. **ArrowJSONFileFormat** (`org.apache.gluten.datasource.ArrowJSONFileFormat`)
   - Main file format class that extends Spark's `FileFormat`
   - Handles schema inference and batch reading
   - Provides fallback to vanilla Spark JSON reader when needed
   - Supports columnar batch processing

2. **ArrowJSONOptionConverter** (`org.apache.gluten.datasource.ArrowJSONOptionConverter`)
   - Converts Spark JSON options to Arrow JSON fragment scan options
   - Configures Arrow's JSON parser settings
   - Handles schema conversion between Spark and Arrow

3. **ArrowJSONTable** (`org.apache.gluten.datasource.v2.ArrowJSONTable`)
   - DataSource V2 table implementation for JSON files
   - Provides schema inference capabilities
   - Creates scan builders for reading data

4. **ArrowJSONScan** (`org.apache.gluten.datasource.v2.ArrowJSONScan`)
   - Implements the scan operation for JSON files
   - Manages partition filters and data filters
   - Creates partition reader factories

5. **ArrowJSONScanBuilder** (`org.apache.gluten.datasource.v2.ArrowJSONScanBuilder`)
   - Builds scan instances with proper configuration
   - Handles schema pruning and filter pushdown

6. **ArrowJSONPartitionReaderFactory** (`org.apache.gluten.datasource.v2.ArrowJSONPartitionReaderFactory`)
   - Creates partition readers for columnar batch reading
   - Handles fallback to row-based reading when necessary
   - Manages Arrow memory allocation and cleanup

### Integration Components

7. **ArrowConvertorRule** (updated)
   - Converts Spark's JSON file format to Arrow JSON file format
   - Validates JSON options for Arrow compatibility
   - Supports both LogicalRelation and DataSourceV2Relation

8. **ArrowScanReplaceRule** (updated)
   - Replaces standard scan executors with Arrow-optimized versions
   - Handles both FileSourceScanExec and BatchScanExec

## Features

### Supported Features

- **JSON Lines (NDJSON)**: Line-delimited JSON format
- **Schema Inference**: Automatic schema detection from JSON files
- **Columnar Processing**: Efficient columnar batch processing
- **Partition Support**: Handles partitioned JSON datasets
- **Filter Pushdown**: Pushes filters down to the scan level
- **Column Pruning**: Reads only required columns

### Limitations

- **Multi-line JSON**: Not supported (must be single-line JSON/NDJSON)
- **Complex Nested Types**: Limited support for deeply nested structures
- **Character Encoding**: Only UTF-8 encoding is supported
- **Parse Mode**: Only permissive mode is supported

## Configuration

Enable Arrow JSON reader by setting:

```scala
spark.conf.set("spark.gluten.sql.native.arrow.reader.enabled", "true")
```

## Usage Examples

### Basic Reading

```scala
// Read JSON file
val df = spark.read.json("path/to/file.json")
df.show()
```

### With Options

```scala
// Read with specific options
val df = spark.read
  .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
  .json("path/to/file.json")
```

### DataSource V2 API

```scala
// Using DataSource V2
val df = spark.read
  .format("json")
  .load("path/to/file.json")
```

## Implementation Details

### Schema Conversion

The implementation converts between Spark SQL types and Arrow types:
- Spark StructType → Arrow Schema
- Handles primitive types, timestamps, and nested structures
- Preserves nullability information

### Memory Management

- Uses Arrow's BufferAllocator for memory allocation
- Implements proper cleanup with recycling iterators
- Supports both on-heap and off-heap memory

### Fallback Mechanism

When Arrow cannot handle certain JSON configurations, the implementation falls back to Spark's vanilla JSON reader:
- Multi-line JSON files
- Unsupported character encodings
- Complex parse modes
- Schema mismatches

### Performance Considerations

1. **Columnar Processing**: Processes data in columnar batches for better CPU cache utilization
2. **Zero-Copy**: Minimizes data copying between Arrow and Spark
3. **Vectorized Operations**: Leverages Arrow's vectorized operations
4. **Memory Pooling**: Reuses Arrow memory pools across operations

## Testing

A test suite is provided in `ArrowJsonScanSuite.scala` that covers:
- Basic JSON file reading
- DataSource V2 integration
- Query execution with filters
- Schema inference

Run tests with:
```bash
mvn test -Dtest=ArrowJsonScanSuite
```

## Comparison with CSV Reader

The JSON reader follows the same architectural pattern as the CSV reader:

| Aspect | CSV Reader | JSON Reader |
|--------|-----------|-------------|
| File Format | ArrowCSVFileFormat | ArrowJSONFileFormat |
| Option Converter | ArrowCSVOptionConverter | ArrowJSONOptionConverter |
| Scan Options | CsvFragmentScanOptions | JsonFragmentScanOptions |
| Header Checking | Yes | No (not applicable) |
| Delimiter Config | Yes | No (JSON structure) |

## Future Enhancements

Potential improvements for future versions:
1. Support for multi-line JSON
2. Enhanced nested type support
3. Compression format support (gzip, bzip2)
4. Streaming JSON support
5. Custom JSON parser configurations

## References

- [Apache Arrow Dataset API](https://arrow.apache.org/docs/cpp/dataset.html)
- [Spark DataSource V2 API](https://spark.apache.org/docs/latest/sql-data-sources-v2.html)
- [JSON Lines Format](https://jsonlines.org/)