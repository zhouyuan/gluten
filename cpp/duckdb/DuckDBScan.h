/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "duckdb.hpp"

namespace gluten {

struct DuckDBScanOptions {
  /// Threads of the scan's DuckDB instance. One instance serves one Spark
  /// task, so this defaults to a single thread. 0 means the DuckDB default
  /// (one thread per core), which is rarely what you want with many
  /// concurrent tasks.
  int64_t threads{1};
  /// DuckDB memory_limit of the scan's instance, e.g. "1GB". Empty keeps the
  /// DuckDB default.
  std::string memoryLimit;
  /// Path of a locally built substrait extension. Empty installs/loads the
  /// community extension instead (requires network access once per host).
  std::string substraitExtensionPath;
};

/// Lists the top-level column names of a parquet file in physical order.
/// The JVM side uses this to compute the positional projection the DuckDB
/// substrait consumer expects.
std::vector<std::string> duckdbDescribeParquet(const std::string& path);

/// Executes one Substrait plan (a single parquet ReadRel with local_files,
/// built by Gluten's JVM side) through DuckDB's from_substrait and streams
/// the result as Arrow data.
class DuckDBScan {
 public:
  explicit DuckDBScan(const DuckDBScanOptions& options);

  /// Runs the plan; call exactly once before next().
  void execute(const std::string& planBytes);

  /// Moves the next batch's schema/array into the given JVM-allocated C
  /// struct shells (Arrow C Data Interface move semantics, the caller owns
  /// and must release them). Returns false on end of stream.
  bool next(struct ArrowSchema* outSchema, struct ArrowArray* outArray);

 private:
  void loadSubstraitExtension(const std::string& extensionPath);

  duckdb::DuckDB db_;
  duckdb::Connection con_;
  std::unique_ptr<duckdb::QueryResult> result_;
};

} // namespace gluten
