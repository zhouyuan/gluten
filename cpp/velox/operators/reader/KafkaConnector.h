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

#include "velox/connectors/Connector.h"
#include "velox/exec/Operator.h"

namespace gluten {

/// Kafka connector for streaming data from Kafka topics
class KafkaConnector : public facebook::velox::connector::Connector {
 public:
  explicit KafkaConnector(
      const std::string& id,
      std::shared_ptr<const facebook::velox::config::ConfigBase> config)
      : Connector(id), config_(std::move(config)) {}

  std::unique_ptr<facebook::velox::connector::DataSource> createDataSource(
      const facebook::velox::RowTypePtr& outputType,
      const std::shared_ptr<facebook::velox::connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<facebook::velox::connector::ColumnHandle>>& columnHandles,
      facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx) override {
    VELOX_UNSUPPORTED("KafkaConnector does not support createDataSource");
  }

  std::unique_ptr<facebook::velox::connector::DataSink> createDataSink(
      facebook::velox::RowTypePtr inputType,
      std::shared_ptr<facebook::velox::connector::ConnectorInsertTableHandle> connectorInsertTableHandle,
      facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx,
      facebook::velox::connector::CommitStrategy commitStrategy) override {
    VELOX_UNSUPPORTED("KafkaConnector does not support createDataSink");
  }

  const std::shared_ptr<const facebook::velox::config::ConfigBase>& getConfig() const {
    return config_;
  }

 private:
  std::shared_ptr<const facebook::velox::config::ConfigBase> config_;
};

/// Factory for creating Kafka data sources
/// This is used by the Velox execution engine to create KafkaReader operators
class KafkaDataSourceFactory {
 public:
  static std::unique_ptr<facebook::velox::exec::SourceOperator> createKafkaReader(
      int32_t operatorId,
      facebook::velox::exec::DriverCtx* driverCtx,
      const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode);
};

} // namespace gluten

// Made with Bob
