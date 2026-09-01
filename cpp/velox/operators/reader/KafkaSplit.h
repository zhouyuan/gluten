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

#include <string>
#include <unordered_map>
#include "velox/connectors/Connector.h"

namespace gluten {

/// Split for Kafka streaming data
/// Contains all necessary information to connect to and consume from a Kafka topic/partition
class KafkaConnectorSplit : public facebook::velox::connector::ConnectorSplit {
 public:
  KafkaConnectorSplit(
      const std::string& connectorId,
      const std::string& brokers,
      const std::string& topic,
      int32_t partition,
      int64_t startOffset,
      int64_t endOffset,
      const std::string& groupId = "",
      const std::unordered_map<std::string, std::string>& additionalProps = {})
      : ConnectorSplit(connectorId),
        brokers_(brokers),
        topic_(topic),
        partition_(partition),
        startOffset_(startOffset),
        endOffset_(endOffset),
        groupId_(groupId),
        additionalProps_(additionalProps) {}

  const std::string& getBrokers() const {
    return brokers_;
  }

  const std::string& getTopic() const {
    return topic_;
  }

  int32_t getPartition() const {
    return partition_;
  }

  int64_t getStartOffset() const {
    return startOffset_;
  }

  int64_t getEndOffset() const {
    return endOffset_;
  }

  const std::string& getGroupId() const {
    return groupId_;
  }

  const std::unordered_map<std::string, std::string>& getAdditionalProps() const {
    return additionalProps_;
  }

  std::string toString() const override {
    return fmt::format(
        "KafkaConnectorSplit[brokers={}, topic={}, partition={}, startOffset={}, endOffset={}]",
        brokers_,
        topic_,
        partition_,
        startOffset_,
        endOffset_);
  }

 private:
  std::string brokers_;
  std::string topic_;
  int32_t partition_;
  int64_t startOffset_;
  int64_t endOffset_;
  std::string groupId_;
  std::unordered_map<std::string, std::string> additionalProps_;
};

} // namespace gluten

// Made with Bob
