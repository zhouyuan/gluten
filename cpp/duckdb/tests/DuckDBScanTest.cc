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

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "DuckDBScan.h"
#include "duckdb/common/arrow/arrow.hpp"

namespace gluten {
namespace {

class DuckDBScanTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = (std::filesystem::temp_directory_path() / "gluten_duckdb_scan_test.parquet").string();
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    auto result = con.Query(
        "COPY (SELECT i::BIGINT AS id, 'name-' || (i % 13) AS name, (i % 100) / 4.0 AS price"
        " FROM range(0, 1000) t(i)) TO '" +
        path_ + "' (FORMAT PARQUET)");
    ASSERT_FALSE(result->HasError()) << result->GetError();
  }

  void TearDown() override {
    std::filesystem::remove(path_);
  }

  /// Produces a Substrait plan for the given query with the substrait
  /// extension's own producer, so the test needs no protobuf dependency.
  /// Returns empty when the extension is unavailable (e.g. no network).
  std::string producePlan(const std::string& query) {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    if (con.Query("LOAD substrait")->HasError() &&
        con.Query("INSTALL substrait FROM community")->HasError()) {
      return {};
    }
    con.Query("LOAD substrait");
    auto result = con.Query("SELECT get_substrait(" + quoted(query) + ")");
    if (result->HasError()) {
      return {};
    }
    auto chunk = result->Fetch();
    return duckdb::StringValue::Get(chunk->GetValue(0, 0));
  }

  static std::string quoted(const std::string& s) {
    std::string escaped = s;
    size_t pos = 0;
    while ((pos = escaped.find('\'', pos)) != std::string::npos) {
      escaped.insert(pos, 1, '\'');
      pos += 2;
    }
    return "'" + escaped + "'";
  }

  std::string path_;
};

TEST_F(DuckDBScanTest, describeParquet) {
  auto columns = duckdbDescribeParquet(path_);
  ASSERT_EQ(columns.size(), 3);
  EXPECT_EQ(columns[0], "id");
  EXPECT_EQ(columns[1], "name");
  EXPECT_EQ(columns[2], "price");
}

TEST_F(DuckDBScanTest, describeParquetMissingFile) {
  EXPECT_THROW(duckdbDescribeParquet("/nonexistent/no.parquet"), std::exception);
}

TEST_F(DuckDBScanTest, executeSubstraitPlan) {
  auto plan = producePlan("SELECT name, id FROM parquet_scan('" + path_ + "') WHERE id < 100");
  if (plan.empty()) {
    GTEST_SKIP() << "substrait extension unavailable (network needed to install it)";
  }
  DuckDBScanOptions options;
  DuckDBScan scan(options);
  scan.execute(plan);
  int64_t rows = 0;
  while (true) {
    ArrowSchema schema;
    ArrowArray array;
    if (!scan.next(&schema, &array)) {
      break;
    }
    EXPECT_EQ(schema.n_children, 2);
    EXPECT_STREQ(schema.children[0]->name, "name");
    EXPECT_STREQ(schema.children[1]->name, "id");
    rows += array.length;
    array.release(&array);
    schema.release(&schema);
  }
  EXPECT_EQ(rows, 100);
}

TEST_F(DuckDBScanTest, invalidPlanFails) {
  auto probe = producePlan("SELECT 1");
  if (probe.empty()) {
    GTEST_SKIP() << "substrait extension unavailable (network needed to install it)";
  }
  DuckDBScanOptions options;
  DuckDBScan scan(options);
  EXPECT_THROW(scan.execute("not a substrait plan"), std::exception);
}

} // namespace
} // namespace gluten
