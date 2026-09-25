/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "operators/plannodes/RollupAggregation.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace gluten {

namespace {

struct AggregateSpec {
  std::string name;
  std::string input;
};

} // namespace

class RollupAggregationTest : public OperatorTestBase {
 protected:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
    // Same registration as Gluten: Spark aggregates override Presto ones, with companion
    // functions.
    aggregate::prestosql::registerAllAggregateFunctions(
        "", /*registerCompanionFunctions=*/true, /*onlyPrestoSignatures=*/false, /*overwrite=*/true);
    functions::aggregate::sparksql::registerAggregateFunctions(
        "", /*registerCompanionFunctions=*/true, /*overwrite=*/true);
    Operator::registerOperator(std::make_unique<RollupAggregationTranslator>());
  }

  void SetUp() override {
    OperatorTestBase::SetUp();
    rowType_ =
        ROW({"a", "b", "c", "x", "y", "d"}, {INTEGER(), VARCHAR(), BIGINT(), BIGINT(), DOUBLE(), DECIMAL(17, 2)});
  }

  std::vector<RowVectorPtr> makeInput(int32_t numBatches, int32_t batchSize) {
    std::vector<RowVectorPtr> batches;
    for (auto batch = 0; batch < numBatches; ++batch) {
      const auto offset = batch * batchSize;
      batches.push_back(makeRowVector(
          rowType_->names(),
          {
              makeFlatVector<int32_t>(
                  batchSize, [&](auto row) { return (offset + row) % 7; }, [&](auto row) { return row % 11 == 0; }),
              makeFlatVector<std::string>(
                  batchSize,
                  [&](auto row) { return fmt::format("str-{}", (offset + row) % 13); },
                  [&](auto row) { return row % 17 == 0; }),
              makeFlatVector<int64_t>(batchSize, [&](auto row) { return (offset + row) % 101; }),
              makeFlatVector<int64_t>(
                  batchSize, [&](auto row) { return offset + row; }, [&](auto row) { return row % 5 == 0; }),
              makeFlatVector<double>(batchSize, [&](auto row) { return (offset + row) * 0.5; }),
              makeFlatVector<int64_t>(
                  batchSize,
                  [&](auto row) { return (offset + row) * 1'001; },
                  [&](auto row) { return row % 3 == 0; },
                  DECIMAL(17, 2)),
          }));
    }
    return batches;
  }

  // Builds a plan: values -> RollupAggregation -> final aggregation.
  core::PlanNodePtr makeRollupPlan(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& keys,
      const std::vector<std::vector<std::string>>& groupingSets,
      const std::vector<AggregateSpec>& aggregates,
      bool parallel = false) {
    std::vector<core::FieldAccessTypedExprPtr> groupingKeys;
    for (const auto& key : keys) {
      groupingKeys.push_back(std::make_shared<core::FieldAccessTypedExpr>(rowType_->findChild(key), key));
    }
    std::vector<std::vector<column_index_t>> sets;
    std::vector<int64_t> groupIds;
    for (auto i = 0; i < groupingSets.size(); ++i) {
      std::vector<column_index_t> set;
      for (const auto& key : groupingSets[i]) {
        set.push_back(std::find(keys.begin(), keys.end(), key) - keys.begin());
      }
      std::sort(set.begin(), set.end());
      sets.push_back(std::move(set));
      groupIds.push_back(i);
    }

    std::vector<std::string> outputNames = keys;
    outputNames.push_back("group_id");
    std::vector<core::AggregationNode::Aggregate> partials;
    std::vector<core::AggregationNode::Aggregate> merges;
    for (auto i = 0; i < aggregates.size(); ++i) {
      const auto& spec = aggregates[i];
      const auto name = fmt::format("a{}", i);
      outputNames.push_back(name);
      std::vector<TypePtr> rawInputTypes{rowType_->findChild(spec.input)};
      auto intermediateType = resolveIntermediateType(spec.name, rawInputTypes);
      partials.push_back(
          {std::make_shared<core::CallTypedExpr>(
               intermediateType,
               std::vector<core::TypedExprPtr>{
                   std::make_shared<core::FieldAccessTypedExpr>(rawInputTypes[0], spec.input)},
               spec.name + "_partial"),
           rawInputTypes,
           nullptr,
           {},
           {}});
      merges.push_back(
          {std::make_shared<core::CallTypedExpr>(
               intermediateType,
               std::vector<core::TypedExprPtr>{std::make_shared<core::FieldAccessTypedExpr>(intermediateType, name)},
               spec.name + "_merge"),
           {intermediateType},
           nullptr,
           {},
           {}});
    }

    std::vector<std::string> finalKeys = keys;
    finalKeys.push_back("group_id");
    std::vector<std::string> finalAggregates;
    std::vector<std::vector<TypePtr>> rawInputTypes;
    for (auto i = 0; i < aggregates.size(); ++i) {
      finalAggregates.push_back(fmt::format("{}(a{})", aggregates[i].name, i));
      rawInputTypes.push_back({rowType_->findChild(aggregates[i].input)});
    }

    PlanBuilder builder;
    builder.values(input, parallel).addNode([&](std::string id, core::PlanNodePtr source) {
      return std::make_shared<RollupAggregationNode>(
          id, groupingKeys, sets, groupIds, BIGINT(), outputNames, partials, merges, std::move(source));
    });
    if (parallel) {
      builder.localPartition(finalKeys);
    }
    return builder.finalAggregation(finalKeys, finalAggregates, rawInputTypes).planNode();
  }

  // Builds the reference plan: values -> GroupId -> partial aggregation ->
  // final aggregation. Like Spark's Expand, the aggregates read copies of their
  // inputs that GroupId doesn't null out even if they are grouping keys.
  core::PlanNodePtr makeReferencePlan(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& keys,
      const std::vector<std::vector<std::string>>& groupingSets,
      const std::vector<AggregateSpec>& aggregates,
      bool parallel = false) {
    std::vector<std::string> projections = rowType_->names();
    std::vector<std::string> inputs;
    std::vector<std::string> calls;
    for (const auto& spec : aggregates) {
      const auto copy = spec.input + "_copy";
      if (std::find(inputs.begin(), inputs.end(), copy) == inputs.end()) {
        inputs.push_back(copy);
        projections.push_back(fmt::format("{} as {}", spec.input, copy));
      }
      calls.push_back(fmt::format("{}({})", spec.name, copy));
    }
    std::vector<std::string> groupIdKeys = keys;
    groupIdKeys.push_back("group_id");
    PlanBuilder builder;
    builder.values(input, parallel)
        .project(projections)
        .groupId(keys, groupingSets, inputs)
        .partialAggregation(groupIdKeys, calls);
    if (parallel) {
      builder.localPartition(groupIdKeys);
    }
    return builder.finalAggregation().planNode();
  }

  // Checks the results against the reference plan and returns the runtime
  // stats of the RollupAggregation.
  std::unordered_map<std::string, RuntimeMetric> testRollup(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& keys,
      const std::vector<std::vector<std::string>>& groupingSets,
      const std::vector<AggregateSpec>& aggregates,
      const std::unordered_map<std::string, std::string>& configs = {}) {
    auto expected = AssertQueryBuilder(makeReferencePlan(input, keys, groupingSets, aggregates)).copyResults(pool());
    auto plan = makeRollupPlan(input, keys, groupingSets, aggregates);
    auto task = AssertQueryBuilder(plan).configs(configs).assertResults(expected);
    return toPlanStats(task->taskStats()).at(plan->sources()[0]->id()).customStats;
  }

  static int64_t statSum(const std::unordered_map<std::string, RuntimeMetric>& stats, std::string_view name) {
    auto it = stats.find(std::string(name));
    return it == stats.end() ? 0 : it->second.sum;
  }

  const std::vector<AggregateSpec>
      kAggregates{{"sum", "x"}, {"count", "x"}, {"avg", "y"}, {"min", "c"}, {"max", "b"}, {"sum", "d"}};

  // Configs that force every grouping set to flush often. The extended memory
  // is large so that the grouping sets keep growing their memory instead of
  // being abandoned.
  const std::unordered_map<std::string, std::string> kFlushConfigs{
      {core::QueryConfig::kMaxPartialAggregationMemory, "1"},
      {core::QueryConfig::kMaxExtendedPartialAggregationMemory, "1073741824"},
      {core::QueryConfig::kAbandonPartialAggregationMinRows, "1000000000"}};

  // Configs that make every grouping set abandon aggregation early.
  const std::unordered_map<std::string, std::string> kAbandonConfigs{
      {core::QueryConfig::kAbandonPartialAggregationMinRows, "10"},
      {core::QueryConfig::kAbandonPartialAggregationMinPct, "0"}};

  RowTypePtr rowType_;
};

TEST_F(RollupAggregationTest, rollup) {
  auto input = makeInput(10, 1'000);
  const std::vector<std::string> keys{"a", "b", "c"};
  const std::vector<std::vector<std::string>> sets{{"a", "b", "c"}, {"a", "b"}, {"a"}, {}};

  // Each grouping set is flushed once at the end.
  auto stats = testRollup(input, keys, sets, kAggregates);
  ASSERT_EQ(statSum(stats, RollupAggregation::kFlushTimes), sets.size());
  ASSERT_EQ(statSum(stats, RollupAggregation::kAbandonedGroupingSets), 0);

  stats = testRollup(input, keys, sets, kAggregates, kFlushConfigs);
  ASSERT_GT(statSum(stats, RollupAggregation::kFlushTimes), sets.size());
  ASSERT_EQ(statSum(stats, RollupAggregation::kAbandonedGroupingSets), 0);

  // All but the global grouping set are abandoned.
  stats = testRollup(input, keys, sets, kAggregates, kAbandonConfigs);
  ASSERT_EQ(statSum(stats, RollupAggregation::kAbandonedGroupingSets), sets.size() - 1);
  ASSERT_GT(statSum(stats, RollupAggregation::kAbandonedRows), 0);
}

TEST_F(RollupAggregationTest, groupingSetChainWithoutGlobal) {
  auto input = makeInput(10, 1'000);
  // Not a prefix chain, and the keys are in a different order than the
  // input columns.
  const std::vector<std::string> keys{"c", "a", "b"};
  const std::vector<std::vector<std::string>> sets{{"c", "b"}, {"b"}};
  testRollup(input, keys, sets, kAggregates);
  testRollup(input, keys, sets, kAggregates, kFlushConfigs);
  testRollup(input, keys, sets, kAggregates, kAbandonConfigs);
}

TEST_F(RollupAggregationTest, duplicateGroupingSets) {
  auto input = makeInput(3, 1'000);
  const std::vector<std::string> keys{"a", "b"};
  const std::vector<std::vector<std::string>> sets{{"a", "b"}, {"a", "b"}, {"a"}, {"a"}, {}, {}};
  testRollup(input, keys, sets, kAggregates);
  testRollup(input, keys, sets, kAggregates, kFlushConfigs);
}

TEST_F(RollupAggregationTest, emptyInput) {
  auto input = makeInput(1, 0);
  const std::vector<std::string> keys{"a", "b"};
  const std::vector<std::vector<std::string>> sets{{"a", "b"}, {"a"}, {}};
  // Like Spark's Expand followed by aggregation, no row is produced for the
  // global grouping set. Velox's GroupId would produce one, so it is not
  // compared against.
  auto result = AssertQueryBuilder(makeRollupPlan(input, keys, sets, kAggregates)).copyResults(pool());
  ASSERT_EQ(result->size(), 0);
}

TEST_F(RollupAggregationTest, multipleDrivers) {
  auto input = makeInput(20, 500);
  const std::vector<std::string> keys{"a", "b", "c"};
  const std::vector<std::vector<std::string>> sets{{"a", "b", "c"}, {"a", "b"}, {"a"}, {}};
  auto expected = AssertQueryBuilder(makeReferencePlan(input, keys, sets, kAggregates, /*parallel=*/true))
                      .maxDrivers(4)
                      .copyResults(pool());
  AssertQueryBuilder(makeRollupPlan(input, keys, sets, kAggregates, /*parallel=*/true))
      .maxDrivers(4)
      .configs(kFlushConfigs)
      .assertResults(expected);
}

TEST_F(RollupAggregationTest, invalidGroupingSets) {
  auto input = makeInput(1, 10);
  const std::vector<std::string> keys{"a", "b"};
  VELOX_ASSERT_THROW(
      makeRollupPlan(input, keys, {{"a"}, {"b"}}, kAggregates),
      "Each grouping set of RollupAggregation must be a subset of the previous one");
  VELOX_ASSERT_THROW(
      makeRollupPlan(input, keys, {{"a", "b"}}, kAggregates), "RollupAggregation requires at least two grouping sets");
}

} // namespace gluten

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
