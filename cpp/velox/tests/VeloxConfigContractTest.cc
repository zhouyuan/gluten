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

#include "config/VeloxConfigContract.h"
#include "utils/Exception.h"
#include "velox/core/QueryConfig.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"

using facebook::velox::core::QueryConfig;
using facebook::velox::functions::sparksql::SparkQueryConfig;

namespace gluten {

namespace {

/// Every Velox query config property Gluten writes in
/// WholeStageResultIterator::getQueryContextConf(). Keep in sync with that
/// function: adding a key there without adding it here only loses coverage, it
/// does not break the build.
///
/// Referencing the Velox constants (rather than string literals) means a Velox
/// rename or removal fails to compile here, and a deregistration fails the
/// assertion below. Between them, both drift modes are covered.
std::vector<std::string> glutenQueryConfigKeys() {
  return {
      QueryConfig::kPreferredOutputBatchRows,
      QueryConfig::kMaxOutputBatchRows,
      QueryConfig::kPreferredOutputBatchBytes,
      QueryConfig::kSessionTimezone,
      QueryConfig::kAdjustTimestampToTimezone,
      QueryConfig::kMaxPartialAggregationMemory,
      QueryConfig::kMaxExtendedPartialAggregationMemory,
      QueryConfig::kAbandonPartialAggregationMinPct,
      QueryConfig::kAbandonPartialAggregationMinRows,
      QueryConfig::kSpillEnabled,
      QueryConfig::kAggregationSpillEnabled,
      QueryConfig::kJoinSpillEnabled,
      QueryConfig::kOrderBySpillEnabled,
      QueryConfig::kWindowSpillEnabled,
      QueryConfig::kMaxSpillLevel,
      QueryConfig::kMaxSpillFileSize,
      QueryConfig::kMaxSpillRunRows,
      QueryConfig::kMaxSpillBytes,
      QueryConfig::kSpillWriteBufferSize,
      QueryConfig::kSpillReadBufferSize,
      QueryConfig::kSpillStartPartitionBit,
      QueryConfig::kSpillNumPartitionBits,
      QueryConfig::kSpillableReservationGrowthPct,
      QueryConfig::kSpillPrefixSortEnabled,
      QueryConfig::kSpillCompressionKind,
      QueryConfig::kHashProbeDynamicFilterPushdownEnabled,
      QueryConfig::kHashProbeBloomFilterPushdownMaxSize,
      QueryConfig::kMaxSplitPreloadPerDriver,
      QueryConfig::kAbandonDedupHashMapMinRows,
      QueryConfig::kAbandonDedupHashMapMinPct,
      QueryConfig::kDriverCpuTimeSliceLimitMs,
      QueryConfig::kThrowExceptionOnDuplicateMapKeys,
      QueryConfig::kExprMaxCompiledRegexes,
      QueryConfig::kQueryTraceEnabled,
      QueryConfig::kQueryTraceDir,
      QueryConfig::kQueryTraceMaxBytes,
      QueryConfig::kQueryTraceTaskRegExp,
      QueryConfig::kOpTraceDirectoryCreateConfig,
      SparkQueryConfig::qualify(SparkQueryConfig::kAnsiEnabled),
      SparkQueryConfig::qualify(SparkQueryConfig::kBloomFilterExpectedNumItems),
      SparkQueryConfig::qualify(SparkQueryConfig::kBloomFilterNumBits),
      SparkQueryConfig::qualify(SparkQueryConfig::kBloomFilterMaxNumBits),
      SparkQueryConfig::qualify(SparkQueryConfig::kBloomFilterMaxNumItems),
      SparkQueryConfig::qualify(SparkQueryConfig::kPartitionId),
      SparkQueryConfig::qualify(SparkQueryConfig::kLegacyDateFormatter),
      SparkQueryConfig::qualify(SparkQueryConfig::kLegacyStatisticalAggregate),
      SparkQueryConfig::qualify(SparkQueryConfig::kJsonIgnoreNullFields),
  };
}

std::unordered_map<std::string, std::string> asConfigMap(const std::vector<std::string>& keys) {
  std::unordered_map<std::string, std::string> configs;
  for (const auto& key : keys) {
    configs[key] = "unused";
  }
  return configs;
}

} // namespace

class VeloxConfigContractTest : public ::testing::Test {};

// The contract: every property Gluten sets is one the linked Velox still reads.
// A failure here names the exact keys that a Velox update stopped honouring.
TEST_F(VeloxConfigContractTest, glutenKeysAreRegisteredInVelox) {
  const auto unknown = unknownVeloxQueryConfigKeys(asConfigMap(glutenQueryConfigKeys()));
  EXPECT_TRUE(unknown.empty()) << "Velox no longer recognizes: " << ::testing::PrintToString(unknown);
}

TEST_F(VeloxConfigContractTest, registriesAreNonEmpty) {
  // Guards against the check silently degenerating to a no-op if Velox ever
  // changes how properties are registered.
  EXPECT_FALSE(QueryConfig::registeredProperties().empty());
  EXPECT_FALSE(SparkQueryConfig::registeredProperties().empty());
  EXPECT_GT(knownVeloxQueryConfigKeys().size(), glutenQueryConfigKeys().size());
}

TEST_F(VeloxConfigContractTest, unqualifiedSparkKeyIsRejected) {
  // SparkQueryConfig keys only take effect when prefixed. Writing the bare name
  // is a real bug that this check must catch.
  const auto unknown = unknownVeloxQueryConfigKeys({{SparkQueryConfig::kLegacyDateFormatter, "true"}});
  ASSERT_EQ(unknown.size(), 1);
  EXPECT_EQ(unknown[0], SparkQueryConfig::kLegacyDateFormatter);
}

TEST_F(VeloxConfigContractTest, unknownKeyIsReported) {
  const auto unknown =
      unknownVeloxQueryConfigKeys({{"no_such_velox_property", "1"}, {QueryConfig::kSpillEnabled, "true"}});
  ASSERT_EQ(unknown.size(), 1);
  EXPECT_EQ(unknown[0], "no_such_velox_property");
}

TEST_F(VeloxConfigContractTest, strictModeThrowsAndLenientModeDoesNot) {
  const std::unordered_map<std::string, std::string> configs{{"no_such_velox_property", "1"}};
  EXPECT_THROW(checkVeloxQueryConfigKeys(configs, /*strict=*/true), GlutenException);
  EXPECT_NO_THROW(checkVeloxQueryConfigKeys(configs, /*strict=*/false));
  // A clean map never throws, strict or not.
  EXPECT_NO_THROW(checkVeloxQueryConfigKeys(asConfigMap(glutenQueryConfigKeys()), /*strict=*/true));
}

} // namespace gluten
