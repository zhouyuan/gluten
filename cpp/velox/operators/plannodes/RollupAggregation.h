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

#pragma once

#include <deque>

#include "velox/core/PlanNode.h"
#include "velox/exec/GroupingSet.h"
#include "velox/exec/Operator.h"

namespace gluten {

/// Partial aggregation over a chain of grouping sets (e.g. ROLLUP) without
/// replicating the input rows the way Expand + partial aggregation does.
///
/// The grouping sets must form a chain in which every set is a subset of the
/// previous one, e.g. ROLLUP(a, b, c) = {(a, b, c), (a, b), (a), ()}. The first
/// (finest) set aggregates the raw input. Every following set aggregates, by
/// merging intermediate states, the output of the set before it. Each set
/// works like a flushable partial aggregation: it may flush or abandon
/// aggregation at any time, so the downstream final aggregation must merge
/// the results.
///
/// The output has the same layout as the Expand + partial aggregation it
/// replaces: all grouping keys (keys not in a row's grouping set are null),
/// then the group id column, then the intermediate aggregation results.
class RollupAggregationNode final : public facebook::velox::core::PlanNode {
 public:
  /// @param groupingKeys All grouping keys, in output order.
  /// @param groupingSets Indices into 'groupingKeys' for each grouping set,
  /// finest first. Each set must be a subset of the previous one.
  /// @param groupIds Group id values, one per grouping set.
  /// @param outputNames Names of the output columns: the grouping keys, the
  /// group id and the aggregates.
  /// @param aggregates Aggregates over the raw input (companion '_partial'
  /// functions), used by the first grouping set.
  /// @param mergeAggregates Aggregates merging intermediate results
  /// (companion '_merge' functions), used by the other grouping sets. Each
  /// must take exactly the output column of the matching aggregate as input.
  RollupAggregationNode(
      const facebook::velox::core::PlanNodeId& id,
      std::vector<facebook::velox::core::FieldAccessTypedExprPtr> groupingKeys,
      std::vector<std::vector<facebook::velox::column_index_t>> groupingSets,
      std::vector<int64_t> groupIds,
      facebook::velox::TypePtr groupIdType,
      std::vector<std::string> outputNames,
      std::vector<facebook::velox::core::AggregationNode::Aggregate> aggregates,
      std::vector<facebook::velox::core::AggregationNode::Aggregate> mergeAggregates,
      facebook::velox::core::PlanNodePtr source);

  const facebook::velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "RollupAggregation";
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("RollupAggregation plan node is not serializable");
  }

  const std::vector<facebook::velox::core::FieldAccessTypedExprPtr>& groupingKeys() const {
    return groupingKeys_;
  }

  const std::vector<std::vector<facebook::velox::column_index_t>>& groupingSets() const {
    return groupingSets_;
  }

  const std::vector<int64_t>& groupIds() const {
    return groupIds_;
  }

  const std::vector<facebook::velox::core::AggregationNode::Aggregate>& aggregates() const {
    return aggregates_;
  }

  const std::vector<facebook::velox::core::AggregationNode::Aggregate>& mergeAggregates() const {
    return mergeAggregates_;
  }

  /// Index of the group id column in the output.
  facebook::velox::column_index_t groupIdChannel() const {
    return groupingKeys_.size();
  }

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<facebook::velox::core::FieldAccessTypedExprPtr> groupingKeys_;
  const std::vector<std::vector<facebook::velox::column_index_t>> groupingSets_;
  const std::vector<int64_t> groupIds_;
  const std::vector<facebook::velox::core::AggregationNode::Aggregate> aggregates_;
  const std::vector<facebook::velox::core::AggregationNode::Aggregate> mergeAggregates_;
  const std::vector<facebook::velox::core::PlanNodePtr> sources_;
  facebook::velox::RowTypePtr outputType_;
};

class RollupAggregation : public facebook::velox::exec::Operator {
 public:
  /// Runtime stat keys.
  static constexpr std::string_view kFlushTimes = "rollupFlushTimes";
  static constexpr std::string_view kAbandonedGroupingSets = "rollupAbandonedGroupingSets";
  static constexpr std::string_view kAbandonedRows = "rollupAbandonedRows";

  RollupAggregation(
      int32_t operatorId,
      facebook::velox::exec::DriverCtx* driverCtx,
      const std::shared_ptr<const RollupAggregationNode>& node);

  void initialize() override;

  bool needsInput() const override;

  void addInput(facebook::velox::RowVectorPtr input) override;

  void noMoreInput() override;

  facebook::velox::RowVectorPtr getOutput() override;

  facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* /*future*/) override {
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

  void close() override;

 private:
  // State of one grouping set.
  struct Level {
    std::unique_ptr<facebook::velox::exec::GroupingSet> groupingSet;
    // Indices of the grouping keys of this set in the output.
    std::vector<facebook::velox::column_index_t> keys;
    // Type of the grouping set result: the keys of this set, then the
    // aggregates.
    facebook::velox::RowTypePtr resultType;
    int64_t groupId;
    facebook::velox::exec::RowContainerIterator iterator;
    // Max memory of the hash table before flushing.
    int64_t maxMemory;
    // True while the hash table content is being produced as output.
    bool flushing{false};
    // True if all input was received and all output was produced.
    bool finished{false};
    // True if aggregation was found to be non-reducing. Input is then
    // converted to intermediate results row by row.
    bool abandoned{false};
    // Input and output rows since the last flush.
    int64_t numInputRows{0};
    int64_t numOutputRows{0};
    // Whether any input was received. A global grouping set only produces a
    // row if it received input, matching Expand + partial aggregation.
    bool receivedInput{false};

    bool isGlobal() const {
      return keys.empty();
    }
  };

  std::unique_ptr<facebook::velox::exec::GroupingSet> createGroupingSet(
      const facebook::velox::RowTypePtr& inputType,
      const std::vector<facebook::velox::column_index_t>& keyChannels,
      const std::vector<facebook::velox::core::AggregationNode::Aggregate>& aggregates);

  // Adds 'input' to the level at 'levelIndex'. For levels after the first,
  // 'input' is in the output layout.
  void addLevelInput(size_t levelIndex, const facebook::velox::RowVectorPtr& input);

  // Emits 'output' of the level at 'levelIndex' and passes it on to the next
  // level.
  facebook::velox::RowVectorPtr emit(size_t levelIndex, facebook::velox::RowVectorPtr output);

  // Converts a grouping set result to the output layout.
  facebook::velox::RowVectorPtr toOutput(const Level& level, const facebook::velox::RowVectorPtr& result);

  // Relabels a batch in the output layout from the previous level for an
  // abandoned level: nulls out the keys that are not in 'level' and sets the
  // group id.
  facebook::velox::RowVectorPtr relabel(const Level& level, const facebook::velox::RowVectorPtr& input);

  // Called when a level has produced all the content of its hash table.
  void finishFlush(Level& level);

  bool abandonEarly(const Level& level, int64_t numOutput) const;

  void maybeIncreaseMemory(Level& level, double aggregationPct);

  std::shared_ptr<const RollupAggregationNode> node_;
  const int64_t maxExtendedPartialAggregationMemoryUsage_;
  const int32_t abandonPartialAggregationMinRows_;
  const int32_t abandonPartialAggregationMinPct_;
  const int64_t maxPartialAggregationMemoryUsage_;

  // Number of grouping keys in the output. The group id column follows them.
  facebook::velox::column_index_t numGroupingKeys_{0};
  std::vector<Level> levels_;
  // Input waiting to be converted row by row when the first level is
  // abandoned.
  facebook::velox::RowVectorPtr pendingInput_;
  // Output produced by abandoned levels, waiting to be returned.
  std::deque<facebook::velox::RowVectorPtr> pendingOutput_;
  bool finished_{false};
};

class RollupAggregationTranslator : public facebook::velox::exec::Operator::PlanNodeTranslator {
 public:
  using facebook::velox::exec::Operator::PlanNodeTranslator::toOperator;

  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx,
      int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override;
};

} // namespace gluten
