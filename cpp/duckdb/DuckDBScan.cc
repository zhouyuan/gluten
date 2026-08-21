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

#include "DuckDBScan.h"

#include <stdexcept>

#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"

namespace gluten {
namespace {

/// Renders a SQL string literal ('...' with quotes doubled).
std::string stringLiteral(const std::string& value) {
  std::string escaped;
  escaped.reserve(value.size() + 2);
  escaped.push_back('\'');
  for (char c : value) {
    if (c == '\'') {
      escaped.push_back('\'');
    }
    escaped.push_back(c);
  }
  escaped.push_back('\'');
  return escaped;
}

/// Renders arbitrary bytes as a DuckDB BLOB literal ('\xAB...'::BLOB).
std::string blobLiteral(const std::string& bytes) {
  static const char* kHex = "0123456789ABCDEF";
  std::string literal;
  literal.reserve(bytes.size() * 4 + 8);
  literal.push_back('\'');
  for (unsigned char c : bytes) {
    literal.push_back('\\');
    literal.push_back('x');
    literal.push_back(kHex[c >> 4]);
    literal.push_back(kHex[c & 0xF]);
  }
  literal += "'::BLOB";
  return literal;
}

void query(duckdb::Connection& con, const std::string& sql) {
  auto result = con.Query(sql);
  if (result->HasError()) {
    throw std::runtime_error("gluten-duckdb: '" + sql + "' failed: " + result->GetError());
  }
}

duckdb::DuckDB openDatabase(const DuckDBScanOptions& options) {
  duckdb::DBConfig config;
  if (!options.substraitExtensionPath.empty()) {
    // A locally built extension is not signed by the DuckDB extension repo.
    config.options.allow_unsigned_extensions = true;
  }
  return duckdb::DuckDB(nullptr, &config);
}

} // namespace

std::vector<std::string> duckdbDescribeParquet(const std::string& path) {
  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  auto result = con.Query(
      "SELECT column_name FROM (DESCRIBE SELECT * FROM parquet_scan(" + stringLiteral(path) + "))");
  if (result->HasError()) {
    throw std::runtime_error("gluten-duckdb: failed to describe parquet file '" + path + "': " + result->GetError());
  }
  std::vector<std::string> names;
  while (auto chunk = result->Fetch()) {
    for (duckdb::idx_t i = 0; i < chunk->size(); i++) {
      names.push_back(chunk->GetValue(0, i).ToString());
    }
  }
  return names;
}

DuckDBScan::DuckDBScan(const DuckDBScanOptions& options) : db_(openDatabase(options)), con_(db_) {
  if (options.threads > 0) {
    query(con_, "SET threads TO " + std::to_string(options.threads));
  }
  if (!options.memoryLimit.empty()) {
    query(con_, "SET memory_limit = " + stringLiteral(options.memoryLimit));
  }
  loadSubstraitExtension(options.substraitExtensionPath);
}

void DuckDBScan::loadSubstraitExtension(const std::string& extensionPath) {
  if (!extensionPath.empty()) {
    query(con_, "LOAD " + stringLiteral(extensionPath));
    return;
  }
  // Fast path: already installed on this host (or statically linked).
  if (!con_.Query("LOAD substrait")->HasError()) {
    return;
  }
  auto installed = con_.Query("INSTALL substrait FROM community");
  if (installed->HasError()) {
    throw std::runtime_error(
        "gluten-duckdb: failed to install the DuckDB substrait community extension "
        "(needs network access once per host, or point "
        "spark.gluten.sql.columnar.duckdb.substraitExtensionPath at a locally built extension): " +
        installed->GetError());
  }
  query(con_, "LOAD substrait");
}

void DuckDBScan::execute(const std::string& planBytes) {
  auto result = con_.SendQuery("SELECT * FROM from_substrait(" + blobLiteral(planBytes) + ")");
  if (result->HasError()) {
    throw std::runtime_error("gluten-duckdb: from_substrait failed: " + result->GetError());
  }
  result_ = std::move(result);
}

bool DuckDBScan::next(struct ArrowSchema* outSchema, struct ArrowArray* outArray) {
  if (result_ == nullptr) {
    throw std::runtime_error("gluten-duckdb: scan is not executing");
  }
  duckdb::unique_ptr<duckdb::DataChunk> chunk;
  do {
    chunk = result_->Fetch();
    if (result_->HasError()) {
      throw std::runtime_error("gluten-duckdb: fetch failed: " + result_->GetError());
    }
    if (chunk == nullptr) {
      return false;
    }
  } while (chunk->size() == 0);
  auto properties = con_.context->GetClientProperties();
  duckdb::ArrowConverter::ToArrowSchema(outSchema, result_->types, result_->names, properties);
  duckdb::unordered_map<duckdb::idx_t, const duckdb::shared_ptr<duckdb::ArrowTypeExtensionData>> extensionTypes;
  duckdb::ArrowConverter::ToArrowArray(*chunk, outArray, properties, extensionTypes);
  return true;
}

} // namespace gluten
