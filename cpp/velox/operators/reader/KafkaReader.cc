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

#include "KafkaReader.h"
#include "KafkaSplit.h"
#include <glog/logging.h>
#include "velox/vector/FlatVector.h"

namespace gluten {

namespace {

// Event callback for Kafka consumer
class KafkaEventCb : public RdKafka::EventCb {
 public:
  void event_cb(RdKafka::Event& event) override {
    switch (event.type()) {
      case RdKafka::Event::EVENT_ERROR:
        LOG(ERROR) << "Kafka error: " << RdKafka::err2str(event.err()) 
                   << " - " << event.str();
        break;
      case RdKafka::Event::EVENT_STATS:
        LOG(INFO) << "Kafka stats: " << event.str();
        break;
      case RdKafka::Event::EVENT_LOG:
        LOG(INFO) << "Kafka log: " << event.str();
        break;
      default:
        LOG(INFO) << "Kafka event: " << event.type() << " - " << event.str();
        break;
    }
  }
};

// Rebalance callback for consumer group
class KafkaRebalanceCb : public RdKafka::RebalanceCb {
 public:
  void rebalance_cb(
      RdKafka::KafkaConsumer* consumer,
      RdKafka::ErrorCode err,
      std::vector<RdKafka::TopicPartition*>& partitions) override {
    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      LOG(INFO) << "Kafka rebalance: assigning " << partitions.size() << " partitions";
      consumer->assign(partitions);
    } else if (err == RdKafka::ERR__REVOKE_PARTITIONS) {
      LOG(INFO) << "Kafka rebalance: revoking " << partitions.size() << " partitions";
      consumer->unassign();
    } else {
      LOG(ERROR) << "Kafka rebalance error: " << RdKafka::err2str(err);
      consumer->unassign();
    }
  }
};

} // anonymous namespace

KafkaReader::KafkaReader(
    int32_t operatorId,
    facebook::velox::exec::DriverCtx* driverCtx,
    const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode)
    : SourceOperator(
          driverCtx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "KafkaReader") {
  LOG(INFO) << "KafkaReader created with operator ID: " << operatorId;
}

KafkaReader::~KafkaReader() {
  cleanup();
}

void KafkaReader::initializeConsumer() {
  if (consumerInitialized_) {
    return;
  }

  std::string errstr;

  // Create global configuration
  conf_.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (!conf_) {
    throw std::runtime_error("Failed to create Kafka global configuration");
  }

  // Set broker list
  if (conf_->set("bootstrap.servers", config_.brokers, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::runtime_error("Failed to set bootstrap.servers: " + errstr);
  }

  // Set group ID if provided
  if (!config_.groupId.empty()) {
    if (conf_->set("group.id", config_.groupId, errstr) != RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to set group.id: " + errstr);
    }
  }

  // Set event callback
  static KafkaEventCb eventCb;
  if (conf_->set("event_cb", &eventCb, errstr) != RdKafka::Conf::CONF_OK) {
    LOG(WARNING) << "Failed to set event callback: " << errstr;
  }

  // Set rebalance callback
  static KafkaRebalanceCb rebalanceCb;
  if (conf_->set("rebalance_cb", &rebalanceCb, errstr) != RdKafka::Conf::CONF_OK) {
    LOG(WARNING) << "Failed to set rebalance callback: " << errstr;
  }

  // Disable auto-commit for manual offset management
  if (conf_->set("enable.auto.commit", "false", errstr) != RdKafka::Conf::CONF_OK) {
    LOG(WARNING) << "Failed to disable auto-commit: " << errstr;
  }

  // Set additional properties
  for (const auto& [key, value] : config_.additionalProps) {
    if (conf_->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
      LOG(WARNING) << "Failed to set property " << key << ": " << errstr;
    }
  }

  // Create consumer
  consumer_.reset(RdKafka::KafkaConsumer::create(conf_.get(), errstr));
  if (!consumer_) {
    throw std::runtime_error("Failed to create Kafka consumer: " + errstr);
  }

  consumerInitialized_ = true;
  LOG(INFO) << "Kafka consumer initialized successfully";
}

void KafkaReader::connectToKafka() {
  if (!consumerInitialized_) {
    initializeConsumer();
  }

  // Create topic partition for assignment
  std::vector<RdKafka::TopicPartition*> partitions;
  RdKafka::TopicPartition* partition = 
      RdKafka::TopicPartition::create(config_.topic, config_.partition, config_.startOffset);
  partitions.push_back(partition);

  // Assign partition
  RdKafka::ErrorCode err = consumer_->assign(partitions);
  if (err != RdKafka::ERR_NO_ERROR) {
    delete partition;
    throw std::runtime_error("Failed to assign partition: " + RdKafka::err2str(err));
  }

  currentOffset_ = config_.startOffset;
  LOG(INFO) << "Connected to Kafka topic: " << config_.topic 
            << ", partition: " << config_.partition
            << ", start offset: " << config_.startOffset;

  delete partition;
}

std::vector<RdKafka::Message*> KafkaReader::pollMessages() {
  std::vector<RdKafka::Message*> messages;
  
  if (!consumer_) {
    return messages;
  }

  int32_t remainingRecords = config_.maxPollRecords;
  auto startTime = std::chrono::steady_clock::now();
  
  while (remainingRecords > 0) {
    // Check if we've exceeded the poll timeout
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - startTime).count();
    if (elapsed >= config_.pollTimeoutMs) {
      break;
    }

    // Poll for a single message
    RdKafka::Message* msg = consumer_->consume(100); // 100ms timeout per consume
    
    if (!msg) {
      continue;
    }

    switch (msg->err()) {
      case RdKafka::ERR_NO_ERROR:
        // Valid message
        messages.push_back(msg);
        currentOffset_ = msg->offset();
        messagesRead_++;
        remainingRecords--;
        
        // Check if we've reached the end offset
        if (config_.endOffset != RdKafka::Topic::OFFSET_END && 
            currentOffset_ >= config_.endOffset) {
          finished_ = true;
          return messages;
        }
        break;

      case RdKafka::ERR__PARTITION_EOF:
        // End of partition
        LOG(INFO) << "Reached end of partition at offset: " << currentOffset_;
        delete msg;
        finished_ = true;
        return messages;

      case RdKafka::ERR__TIMED_OUT:
        // Timeout, no message available
        delete msg;
        break;

      default:
        // Error
        LOG(ERROR) << "Kafka consume error: " << msg->errstr();
        delete msg;
        break;
    }
  }

  return messages;
}

facebook::velox::RowVectorPtr KafkaReader::convertMessagesToRowVector(
    const std::vector<RdKafka::Message*>& messages) {
  if (messages.empty()) {
    return nullptr;
  }

  auto rowType = outputType_->asRow();
  size_t numRows = messages.size();
  
  // Create vectors for each column
  std::vector<facebook::velox::VectorPtr> childVectors;
  for (size_t i = 0; i < rowType.size(); ++i) {
    childVectors.push_back(createColumnVector(rowType.childAt(i), messages, i));
  }

  // Create and return RowVector
  return std::make_shared<facebook::velox::RowVector>(
      pool(),
      rowType.asRow(),
      facebook::velox::BufferPtr(nullptr),
      numRows,
      childVectors);
}

facebook::velox::VectorPtr KafkaReader::createColumnVector(
    const facebook::velox::TypePtr& type,
    const std::vector<RdKafka::Message*>& messages,
    size_t columnIndex) {
  size_t numRows = messages.size();
  
  // Handle common Kafka message fields based on column index
  // Column 0: key (VARBINARY)
  // Column 1: value (VARBINARY)
  // Column 2: topic (VARCHAR)
  // Column 3: partition (INTEGER)
  // Column 4: offset (BIGINT)
  // Column 5: timestamp (BIGINT)

  if (type->isVarbinary() || type->isVarchar()) {
    auto flatVector = std::dynamic_pointer_cast<facebook::velox::FlatVector<facebook::velox::StringView>>(
        facebook::velox::BaseVector::create(type, numRows, pool()));
    
    for (size_t i = 0; i < numRows; ++i) {
      const auto* msg = messages[i];
      
      if (columnIndex == 0) {
        // Key
        if (msg->key()) {
          flatVector->set(i, facebook::velox::StringView(
              static_cast<const char*>(msg->key()->c_str()), msg->key()->size()));
        } else {
          flatVector->setNull(i, true);
        }
      } else if (columnIndex == 1) {
        // Value
        if (msg->payload()) {
          flatVector->set(i, facebook::velox::StringView(
              static_cast<const char*>(msg->payload()), msg->len()));
        } else {
          flatVector->setNull(i, true);
        }
      } else if (columnIndex == 2) {
        // Topic
        flatVector->set(i, facebook::velox::StringView(msg->topic_name()));
      }
    }
    
    return flatVector;
  } else if (type->isInteger()) {
    auto flatVector = std::dynamic_pointer_cast<facebook::velox::FlatVector<int32_t>>(
        facebook::velox::BaseVector::create(type, numRows, pool()));
    
    for (size_t i = 0; i < numRows; ++i) {
      const auto* msg = messages[i];
      
      if (columnIndex == 3) {
        // Partition
        flatVector->set(i, msg->partition());
      }
    }
    
    return flatVector;
  } else if (type->isBigint()) {
    auto flatVector = std::dynamic_pointer_cast<facebook::velox::FlatVector<int64_t>>(
        facebook::velox::BaseVector::create(type, numRows, pool()));
    
    for (size_t i = 0; i < numRows; ++i) {
      const auto* msg = messages[i];
      
      if (columnIndex == 4) {
        // Offset
        flatVector->set(i, msg->offset());
      } else if (columnIndex == 5) {
        // Timestamp
        flatVector->set(i, msg->timestamp().timestamp);
      }
    }
    
    return flatVector;
  }
  
  // Default: create empty vector
  return facebook::velox::BaseVector::create(type, numRows, pool());
}

void KafkaReader::commitOffset(int64_t offset) {
  if (!consumer_) {
    return;
  }

  std::vector<RdKafka::TopicPartition*> partitions;
  RdKafka::TopicPartition* partition = 
      RdKafka::TopicPartition::create(config_.topic, config_.partition, offset + 1);
  partitions.push_back(partition);

  RdKafka::ErrorCode err = consumer_->commitSync(partitions);
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(ERROR) << "Failed to commit offset: " << RdKafka::err2str(err);
  } else {
    LOG(INFO) << "Committed offset: " << offset;
  }

  delete partition;
}

void KafkaReader::cleanup() {
  if (consumer_) {
    // Commit final offset
    if (currentOffset_ > 0) {
      commitOffset(currentOffset_);
    }
    
    // Close consumer
    consumer_->close();
    consumer_.reset();
    LOG(INFO) << "Kafka consumer closed. Total messages read: " << messagesRead_;
  }
  
  conf_.reset();
  tconf_.reset();
}

facebook::velox::RowVectorPtr KafkaReader::getOutput() {
  if (finished_ || !hasSplit_) {
    return nullptr;
  }

  // Initialize consumer on first call
  if (!consumerInitialized_) {
    try {
      connectToKafka();
    } catch (const std::exception& e) {
      LOG(ERROR) << "Failed to connect to Kafka: " << e.what();
      finished_ = true;
      return nullptr;
    }
  }

  // Poll messages from Kafka
  auto messages = pollMessages();
  
  if (messages.empty()) {
    // No messages available, but not finished yet
    if (!finished_) {
      return nullptr;
    }
    // Finished reading
    return nullptr;
  }

  // Convert messages to RowVector
  auto result = convertMessagesToRowVector(messages);
  
  // Clean up messages
  for (auto* msg : messages) {
    delete msg;
  }

  // Commit offset periodically (every batch)
  if (currentOffset_ > 0) {
    commitOffset(currentOffset_);
  }

  return result;
}

facebook::velox::BlockingReason KafkaReader::isBlocked(
    facebook::velox::ContinueFuture* future) {
  // Kafka consumer is non-blocking with timeout
  return facebook::velox::BlockingReason::kNotBlocked;
}

bool KafkaReader::isFinished() {
  return (noMoreSplits_ && !hasSplit_) || finished_;
}

void KafkaReader::addSplit(
    std::shared_ptr<facebook::velox::connector::ConnectorSplit> split) {
  // Extract Kafka configuration from split
  auto kafkaSplit = std::dynamic_pointer_cast<const KafkaConnectorSplit>(split);
  if (!kafkaSplit) {
    throw std::runtime_error("Split is not a KafkaConnectorSplit");
  }
  
  hasSplit_ = true;
  
  // Populate config from split
  config_.brokers = kafkaSplit->getBrokers();
  config_.topic = kafkaSplit->getTopic();
  config_.partition = kafkaSplit->getPartition();
  config_.startOffset = kafkaSplit->getStartOffset();
  config_.endOffset = kafkaSplit->getEndOffset();
  config_.groupId = kafkaSplit->getGroupId();
  config_.additionalProps = kafkaSplit->getAdditionalProps();
  
  LOG(INFO) << "Added Kafka split: " << kafkaSplit->toString();
}

void KafkaReader::noMoreSplits() {
  noMoreSplits_ = true;
  LOG(INFO) << "No more splits for KafkaReader";
}

} // namespace gluten

// Made with Bob
