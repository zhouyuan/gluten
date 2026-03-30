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

#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <string>
#include <unordered_map>
#include "velox/connectors/Connector.h"
#include "velox/exec/Operator.h"
#include "velox/vector/ComplexVector.h"

namespace gluten {

/// Configuration for Kafka consumer
struct KafkaReaderConfig {
  std::string brokers;
  std::string topic;
  int32_t partition;
  int64_t startOffset;
  int64_t endOffset;
  std::string groupId;
  int32_t maxPollRecords;
  int32_t pollTimeoutMs;
  std::unordered_map<std::string, std::string> additionalProps;

  KafkaReaderConfig()
      : partition(0),
        startOffset(RdKafka::Topic::OFFSET_BEGINNING),
        endOffset(RdKafka::Topic::OFFSET_END),
        maxPollRecords(1000),
        pollTimeoutMs(1000) {}
};

/// Kafka reader operator for streaming Kafka data
/// Integrates with librdkafka to consume messages from Kafka topics
class KafkaReader : public facebook::velox::exec::SourceOperator {
 public:
  KafkaReader(
      int32_t operatorId,
      facebook::velox::exec::DriverCtx* driverCtx,
      const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode);

  ~KafkaReader() override;

  facebook::velox::RowVectorPtr getOutput() override;

  facebook::velox::BlockingReason isBlocked(
      facebook::velox::ContinueFuture* future) override;

  bool isFinished() override;

  void addSplit(std::shared_ptr<facebook::velox::connector::ConnectorSplit> split) override;

  void noMoreSplits() override;

 private:
  /// Initialize Kafka consumer with configuration
  void initializeConsumer();

  /// Connect to Kafka broker and subscribe to topic/partition
  void connectToKafka();

  /// Poll messages from Kafka
  std::vector<RdKafka::Message*> pollMessages();

  /// Convert Kafka messages to RowVector format
  facebook::velox::RowVectorPtr convertMessagesToRowVector(
      const std::vector<RdKafka::Message*>& messages);

  /// Handle offset management
  void commitOffset(int64_t offset);

  /// Cleanup Kafka resources
  void cleanup();

  /// Create vector for a specific column based on type
  facebook::velox::VectorPtr createColumnVector(
      const facebook::velox::TypePtr& type,
      const std::vector<RdKafka::Message*>& messages,
      size_t columnIndex);

  // Kafka consumer and configuration
  std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
  std::unique_ptr<RdKafka::Conf> conf_;
  std::unique_ptr<RdKafka::Conf> tconf_;
  KafkaReaderConfig config_;

  // State management
  bool noMoreSplits_ = false;
  bool hasSplit_ = false;
  bool consumerInitialized_ = false;
  bool finished_ = false;
  int64_t currentOffset_ = 0;
  int64_t messagesRead_ = 0;

  // Buffer for batching
  std::vector<RdKafka::Message*> messageBuffer_;
  size_t maxBatchSize_ = 1000;
};

} // namespace gluten
