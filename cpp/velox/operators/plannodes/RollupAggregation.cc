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

#include <algorithm>
#include <numeric>

#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/Task.h"

using namespace facebook::velox;

namespace gluten {

namespace {

RowTypePtr makeOutputType(
    const std::vector<core::FieldAccessTypedExprPtr>& groupingKeys,
    const TypePtr& groupIdType,
    const std::vector<std::string>& outputNames,
    const std::vector<core::AggregationNode::Aggregate>& aggregates) {
  VELOX_USER_CHECK_EQ(
      outputNames.size(),
      groupingKeys.size() + 1 + aggregates.size(),
      "RollupAggregation output names must cover the grouping keys, the group id and the aggregates");
  std::vector<TypePtr> types;
  types.reserve(outputNames.size());
  for (const auto& key : groupingKeys) {
    types.push_back(key->type());
  }
  types.push_back(groupIdType);
  for (const auto& aggregate : aggregates) {
    types.push_back(aggregate.call->type());
  }
  return ROW(std::vector<std::string>(outputNames), std::move(types));
}

} // namespace

RollupAggregationNode::RollupAggregationNode(
    const core::PlanNodeId& id,
    std::vector<core::FieldAccessTypedExprPtr> groupingKeys,
    std::vector<std::vector<column_index_t>> groupingSets,
    std::vector<int64_t> groupIds,
    TypePtr groupIdType,
    std::vector<std::string> outputNames,
    std::vector<core::AggregationNode::Aggregate> aggregates,
    std::vector<core::AggregationNode::Aggregate> mergeAggregates,
    core::PlanNodePtr source)
    : core::PlanNode(id),
      groupingKeys_(std::move(groupingKeys)),
      groupingSets_(std::move(groupingSets)),
      groupIds_(std::move(groupIds)),
      aggregates_(std::move(aggregates)),
      mergeAggregates_(std::move(mergeAggregates)),
      sources_{std::move(source)},
      outputType_(makeOutputType(groupingKeys_, groupIdType, outputNames, aggregates_)) {
  VELOX_USER_CHECK(
      groupIdType->kind() == TypeKind::INTEGER || groupIdType->kind() == TypeKind::BIGINT,
      "RollupAggregation group id must be INTEGER or BIGINT, got {}",
      groupIdType->toString());
  VELOX_USER_CHECK_GT(groupingSets_.size(), 1, "RollupAggregation requires at least two grouping sets");
  VELOX_USER_CHECK_EQ(groupingSets_.size(), groupIds_.size(), "RollupAggregation requires one group id per set");
  VELOX_USER_CHECK_EQ(aggregates_.size(), mergeAggregates_.size());

  for (auto i = 0; i < groupingSets_.size(); ++i) {
    const auto& set = groupingSets_[i];
    for (auto j = 0; j < set.size(); ++j) {
      VELOX_USER_CHECK_LT(set[j], groupingKeys_.size(), "Grouping set key index out of range");
      VELOX_USER_CHECK(j == 0 || set[j - 1] < set[j], "Grouping set key indices must be sorted and unique");
    }
    if (i > 0) {
      const auto& previous = groupingSets_[i - 1];
      VELOX_USER_CHECK(
          std::includes(previous.begin(), previous.end(), set.begin(), set.end()),
          "Each grouping set of RollupAggregation must be a subset of the previous one");
    }
  }

  for (auto i = 0; i < aggregates_.size(); ++i) {
    for (const auto* aggregate : {&aggregates_[i], &mergeAggregates_[i]}) {
      VELOX_USER_CHECK_NULL(aggregate->mask, "RollupAggregation does not support aggregation masks");
      VELOX_USER_CHECK(!aggregate->distinct, "RollupAggregation does not support distinct aggregations");
      VELOX_USER_CHECK(aggregate->sortingKeys.empty(), "RollupAggregation does not support sorted aggregations");
    }
    const auto& mergeInputs = mergeAggregates_[i].call->inputs();
    VELOX_USER_CHECK_EQ(mergeInputs.size(), 1, "Merge aggregate must have a single input");
    const auto field = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(mergeInputs[0]);
    VELOX_USER_CHECK(
        field != nullptr && field->name() == outputType_->nameOf(groupIdChannel() + 1 + i),
        "Merge aggregate must take the output of the matching aggregate as input");
    VELOX_USER_CHECK(
        mergeAggregates_[i].call->type()->equivalent(*aggregates_[i].call->type()),
        "Merge aggregate must produce the same type as the matching aggregate");
  }
}

void RollupAggregationNode::addDetails(std::stringstream& stream) const {
  for (auto i = 0; i < groupingSets_.size(); ++i) {
    if (i > 0) {
      stream << ", ";
    }
    stream << groupIds_[i] << ":[";
    for (auto j = 0; j < groupingSets_[i].size(); ++j) {
      if (j > 0) {
        stream << ", ";
      }
      stream << groupingKeys_[groupingSets_[i][j]]->name();
    }
    stream << "]";
  }
  for (auto i = 0; i < aggregates_.size(); ++i) {
    stream << ", " << outputType_->nameOf(groupIdChannel() + 1 + i) << " := " << aggregates_[i].call->toString();
  }
}

RollupAggregation::RollupAggregation(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const RollupAggregationNode>& node)
    : exec::Operator(driverCtx, node->outputType(), operatorId, node->id(), node->name()),
      node_(node),
      maxExtendedPartialAggregationMemoryUsage_(driverCtx->queryConfig().maxExtendedPartialAggregationMemoryUsage()),
      abandonPartialAggregationMinRows_(driverCtx->queryConfig().abandonPartialAggregationMinRows()),
      abandonPartialAggregationMinPct_(driverCtx->queryConfig().abandonPartialAggregationMinPct()),
      maxPartialAggregationMemoryUsage_(driverCtx->queryConfig().maxPartialAggregationMemoryUsage()) {}

void RollupAggregation::initialize() {
  exec::Operator::initialize();

  const auto& inputType = node_->sources()[0]->outputType();
  const auto numKeys = node_->groupingKeys().size();
  numGroupingKeys_ = numKeys;
  const auto numAggregates = node_->aggregates().size();

  std::vector<column_index_t> keyInputChannels;
  keyInputChannels.reserve(numKeys);
  for (const auto& key : node_->groupingKeys()) {
    keyInputChannels.push_back(exec::exprToChannel(key.get(), inputType));
  }

  const auto& groupingSets = node_->groupingSets();
  levels_.resize(groupingSets.size());
  for (auto i = 0; i < groupingSets.size(); ++i) {
    auto& level = levels_[i];
    level.keys = groupingSets[i];
    level.groupId = node_->groupIds()[i];
    level.maxMemory = maxPartialAggregationMemoryUsage_;

    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (auto key : level.keys) {
      names.push_back(outputType_->nameOf(key));
      types.push_back(outputType_->childAt(key));
    }
    for (auto j = 0; j < numAggregates; ++j) {
      const auto channel = node_->groupIdChannel() + 1 + j;
      names.push_back(outputType_->nameOf(channel));
      types.push_back(outputType_->childAt(channel));
    }
    level.resultType = ROW(std::move(names), std::move(types));

    if (i == 0) {
      // The finest grouping set aggregates the raw input.
      std::vector<column_index_t> keyChannels;
      keyChannels.reserve(level.keys.size());
      for (auto key : level.keys) {
        keyChannels.push_back(keyInputChannels[key]);
      }
      level.groupingSet = createGroupingSet(inputType, keyChannels, node_->aggregates());
    } else {
      // The other grouping sets merge output of the previous one, which is in
      // the output layout.
      level.groupingSet = createGroupingSet(outputType_, level.keys, node_->mergeAggregates());
    }
  }

  node_.reset();
}

std::unique_ptr<exec::GroupingSet> RollupAggregation::createGroupingSet(
    const RowTypePtr& inputType,
    const std::vector<column_index_t>& keyChannels,
    const std::vector<core::AggregationNode::Aggregate>& aggregates) {
  const auto& queryConfig = operatorCtx_->driverCtx()->queryConfig();
  const auto numKeys = keyChannels.size();

  std::vector<exec::AggregateInfo> aggregateInfos;
  aggregateInfos.reserve(aggregates.size());
  for (auto i = 0; i < aggregates.size(); ++i) {
    const auto& aggregate = aggregates[i];
    exec::AggregateInfo info;
    for (const auto& arg : aggregate.call->inputs()) {
      if (auto field = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(arg)) {
        info.inputs.push_back(inputType->getChildIdx(field->name()));
        info.constantInputs.push_back(nullptr);
      } else if (auto constant = std::dynamic_pointer_cast<const core::ConstantTypedExpr>(arg)) {
        info.inputs.push_back(kConstantChannel);
        info.constantInputs.push_back(constant->toConstantVector(pool()));
      } else {
        VELOX_USER_FAIL("RollupAggregation input must be a field access or a constant: {}", arg->toString());
      }
    }
    const auto& name = aggregate.call->name();
    info.intermediateType = exec::resolveIntermediateType(name, aggregate.rawInputTypes);
    // All grouping sets produce intermediate results.
    info.function = exec::Aggregate::create(
        name,
        core::AggregationNode::Step::kPartial,
        aggregate.rawInputTypes,
        outputType_->childAt(outputType_->size() - aggregates.size() + i),
        queryConfig);
    if (!info.constantInputs.empty()) {
      info.function->setConstantInputs(info.constantInputs);
    }
    info.output = numKeys + i;
    aggregateInfos.push_back(std::move(info));
  }

  std::vector<column_index_t> groupingKeyOutputProjections(numKeys);
  std::iota(groupingKeyOutputProjections.begin(), groupingKeyOutputProjections.end(), 0);

  return std::make_unique<exec::GroupingSet>(
      inputType,
      exec::createVectorHashers(inputType, keyChannels),
      /*preGroupedKeys=*/std::vector<column_index_t>{},
      std::move(groupingKeyOutputProjections),
      std::move(aggregateInfos),
      /*ignoreNullKeys=*/false,
      /*isPartial=*/true,
      // Merging levels use the companion '_merge' functions, which take the
      // intermediate results as raw input.
      /*isRawInput=*/true,
      /*globalGroupingSets=*/std::vector<vector_size_t>{},
      /*groupIdChannel=*/std::nullopt,
      /*spillConfig=*/nullptr,
      &nonReclaimableSection_,
      &queryConfig,
      pool(),
      spillStats_.get());
}

bool RollupAggregation::needsInput() const {
  if (noMoreInput_ || pendingInput_ != nullptr || !pendingOutput_.empty()) {
    return false;
  }
  return std::none_of(levels_.begin(), levels_.end(), [](const auto& level) { return level.flushing; });
}

void RollupAggregation::addInput(RowVectorPtr input) {
  VELOX_CHECK_NULL(pendingInput_);
  if (levels_[0].abandoned) {
    pendingInput_ = std::move(input);
    return;
  }
  addLevelInput(0, input);
}

void RollupAggregation::addLevelInput(size_t levelIndex, const RowVectorPtr& input) {
  auto& level = levels_[levelIndex];
  VELOX_CHECK(!level.flushing && !level.finished && !level.abandoned);
  if (input->size() == 0) {
    return;
  }
  level.receivedInput = true;
  level.groupingSet->addInput(input, /*mayPushdown=*/false);
  level.numInputRows += input->size();
  // A global grouping set holds a single group; it is only flushed at the end.
  if (!level.isGlobal() &&
      (abandonEarly(level, level.groupingSet->numDistinct()) || level.groupingSet->isPartialFull(level.maxMemory))) {
    level.flushing = true;
  }
}

void RollupAggregation::noMoreInput() {
  exec::Operator::noMoreInput();
  // Release the extra reserved memory right after processing all the inputs.
  pool()->release();
}

RowVectorPtr RollupAggregation::getOutput() {
  if (finished_) {
    return nullptr;
  }
  const auto& queryConfig = operatorCtx_->driverCtx()->queryConfig();
  for (;;) {
    if (!pendingOutput_.empty()) {
      auto output = std::move(pendingOutput_.front());
      pendingOutput_.pop_front();
      return output;
    }

    // Flush the coarsest flushing level first. Its input comes from the finer
    // levels, so it must be drained before they can produce more output.
    auto flushing = std::find_if(levels_.rbegin(), levels_.rend(), [](const auto& level) { return level.flushing; });
    if (flushing != levels_.rend()) {
      auto& level = *flushing;
      const auto levelIndex = std::distance(levels_.begin(), flushing.base()) - 1;
      std::optional<uint64_t> rowSize;
      if (auto estimate = level.groupingSet->estimateOutputRowSize()) {
        rowSize = estimate.value();
      }
      const auto maxOutputRows = level.isGlobal() ? 1 : outputBatchRows(rowSize);
      auto result = BaseVector::create<RowVector>(level.resultType, maxOutputRows, pool());
      if (level.groupingSet->getOutput(
              maxOutputRows, queryConfig.preferredOutputBatchBytes(), level.iterator, result)) {
        level.numOutputRows += result->size();
        return emit(levelIndex, toOutput(level, result));
      }
      finishFlush(level);
      continue;
    }

    if (pendingInput_ != nullptr) {
      // The finest level is abandoned: convert the input row by row.
      auto& level = levels_[0];
      const auto numRows = pendingInput_->size();
      auto result = BaseVector::create<RowVector>(level.resultType, numRows, pool());
      level.groupingSet->toIntermediate(pendingInput_, result);
      pendingInput_ = nullptr;
      addRuntimeStat(std::string(kAbandonedRows), RuntimeCounter(numRows));
      return emit(0, toOutput(level, result));
    }

    if (!noMoreInput_) {
      return nullptr;
    }

    // All input was received. Produce the remaining content of the levels,
    // finest first, since each level feeds the next one.
    auto unfinished = std::find_if(levels_.begin(), levels_.end(), [](const auto& level) { return !level.finished; });
    if (unfinished == levels_.end()) {
      finished_ = true;
      return nullptr;
    }
    if (unfinished->abandoned || !unfinished->receivedInput) {
      unfinished->finished = true;
    } else {
      unfinished->flushing = true;
    }
  }
}

RowVectorPtr RollupAggregation::emit(size_t levelIndex, RowVectorPtr output) {
  auto input = output;
  for (auto i = levelIndex + 1; i < levels_.size(); ++i) {
    auto& level = levels_[i];
    if (!level.abandoned) {
      addLevelInput(i, input);
      break;
    }
    // An abandoned level passes the intermediate results through.
    input = relabel(level, input);
    addRuntimeStat(std::string(kAbandonedRows), RuntimeCounter(input->size()));
    pendingOutput_.push_back(input);
  }
  return output;
}

RowVectorPtr RollupAggregation::toOutput(const Level& level, const RowVectorPtr& result) {
  const auto numRows = result->size();
  const auto numKeys = numGroupingKeys_;
  std::vector<VectorPtr> children;
  children.reserve(outputType_->size());
  size_t keyIndex = 0;
  for (column_index_t i = 0; i < numKeys; ++i) {
    if (keyIndex < level.keys.size() && level.keys[keyIndex] == i) {
      children.push_back(result->childAt(keyIndex++));
    } else {
      children.push_back(BaseVector::createNullConstant(outputType_->childAt(i), numRows, pool()));
    }
  }
  const auto& groupIdType = outputType_->childAt(numKeys);
  children.push_back(BaseVector::createConstant(
      groupIdType,
      groupIdType->kind() == TypeKind::INTEGER ? Variant(static_cast<int32_t>(level.groupId)) : Variant(level.groupId),
      numRows,
      pool()));
  for (auto i = level.keys.size(); i < result->childrenSize(); ++i) {
    children.push_back(result->childAt(i));
  }
  return std::make_shared<RowVector>(pool(), outputType_, nullptr, numRows, std::move(children));
}

RowVectorPtr RollupAggregation::relabel(const Level& level, const RowVectorPtr& input) {
  const auto numRows = input->size();
  const auto numKeys = numGroupingKeys_;
  auto children = input->children();
  size_t keyIndex = 0;
  for (column_index_t i = 0; i < numKeys; ++i) {
    if (keyIndex < level.keys.size() && level.keys[keyIndex] == i) {
      ++keyIndex;
    } else {
      children[i] = BaseVector::createNullConstant(outputType_->childAt(i), numRows, pool());
    }
  }
  const auto& groupIdType = outputType_->childAt(numKeys);
  children[numKeys] = BaseVector::createConstant(
      groupIdType,
      groupIdType->kind() == TypeKind::INTEGER ? Variant(static_cast<int32_t>(level.groupId)) : Variant(level.groupId),
      numRows,
      pool());
  return std::make_shared<RowVector>(pool(), outputType_, nullptr, numRows, std::move(children));
}

void RollupAggregation::finishFlush(Level& level) {
  const auto levelIndex = &level - levels_.data();
  level.iterator.reset();
  level.flushing = false;
  addRuntimeStat(std::string(kFlushTimes), RuntimeCounter(1));

  // The level is done once no more input can arrive: all input was received
  // and all the finer levels are done.
  if (noMoreInput_ && (levelIndex == 0 || levels_[levelIndex - 1].finished)) {
    level.finished = true;
    level.groupingSet->resetTable(/*freeTable=*/true);
    return;
  }

  VELOX_CHECK(!level.isGlobal());
  const double aggregationPct = level.numInputRows == 0 ? 0 : (level.numOutputRows * 1.0) / level.numInputRows * 100;
  level.groupingSet->resetTable(/*freeTable=*/false);
  maybeIncreaseMemory(level, aggregationPct);
  level.numInputRows = 0;
  level.numOutputRows = 0;
}

bool RollupAggregation::abandonEarly(const Level& level, int64_t numOutput) const {
  return level.numInputRows > abandonPartialAggregationMinRows_ &&
      100 * numOutput / level.numInputRows >= abandonPartialAggregationMinPct_;
}

void RollupAggregation::maybeIncreaseMemory(Level& level, double aggregationPct) {
  // Same policy as the partial HashAggregation: if more than this many rows
  // are unique at full memory, give up on aggregating this level.
  constexpr int32_t kPartialMinFinalPct = 40;
  if (abandonEarly(level, level.numOutputRows) ||
      (aggregationPct > kPartialMinFinalPct && level.maxMemory >= maxExtendedPartialAggregationMemoryUsage_)) {
    level.groupingSet->abandonPartialAggregation();
    level.abandoned = true;
    pool()->release();
    addRuntimeStat(std::string(kAbandonedGroupingSets), RuntimeCounter(1));
    return;
  }
  const int64_t extendedMemory = std::min(level.maxMemory * 2, maxExtendedPartialAggregationMemoryUsage_);
  const int64_t memoryToReserve =
      std::max<int64_t>(0, extendedMemory - static_cast<int64_t>(level.groupingSet->allocatedBytes()));
  if (!pool()->maybeReserve(memoryToReserve)) {
    return;
  }
  level.maxMemory = extendedMemory;
}

void RollupAggregation::close() {
  pendingInput_ = nullptr;
  pendingOutput_.clear();
  levels_.clear();
  exec::Operator::close();
}

std::unique_ptr<exec::Operator>
RollupAggregationTranslator::toOperator(exec::DriverCtx* ctx, int32_t id, const core::PlanNodePtr& node) {
  if (auto rollupNode = std::dynamic_pointer_cast<const RollupAggregationNode>(node)) {
    return std::make_unique<RollupAggregation>(id, ctx, rollupNode);
  }
  return nullptr;
}

} // namespace gluten
