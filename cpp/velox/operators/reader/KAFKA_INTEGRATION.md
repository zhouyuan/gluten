# Kafka Reader Integration Guide

## Overview

This document explains how Kafka streams are mapped to use the KafkaReader in the Gluten Velox backend. The integration allows reading streaming data from Kafka topics through the Substrait plan.

## Architecture

### Components

1. **KafkaSplit** (`KafkaSplit.h`) - Connector split containing Kafka connection parameters
2. **KafkaConnector** (`KafkaConnector.h/cc`) - Velox connector for Kafka streams
3. **KafkaReader** (`KafkaReader.h/cc`) - Source operator that reads from Kafka using librdkafka
4. **SubstraitToVeloxPlan** - Modified to handle `stream_kafka()` in ReadRel

### Flow

```
Substrait ReadRel (stream_kafka=true)
    ↓
SubstraitToVeloxPlanConverter::toVeloxPlan()
    ↓
constructKafkaStreamNode()
    ↓
TableScanNode with kKafkaConnectorId
    ↓
KafkaSplit created with broker/topic/partition info
    ↓
KafkaReader operator consumes messages
    ↓
RowVector output
```

## Key Changes

### 1. SubstraitToVeloxPlan.cc

The `getStreamIndex()` method now returns -1 for Kafka streams, but they are handled specially:

```cpp
// Line 1684-1690
int32_t SubstraitToVeloxPlanConverter::getStreamIndex(const ::substrait::ReadRel& sRead) {
  // Check if this is a Kafka stream
  if (sRead.stream_kafka()) {
    // For Kafka streams, we don't use the iterator pattern
    // Return -1 to indicate this should be handled as a regular scan
    return -1;
  }
  // ... rest of the method
}
```

In `toVeloxPlan()`, Kafka streams are now detected and routed to `constructKafkaStreamNode()`:

```cpp
// Line 1440-1452
core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::ReadRel& readRel) {
  // ... validation code ...
  
  // Check if this is a Kafka stream - handle it specially
  if (readRel.stream_kafka()) {
    return constructKafkaStreamNode(readRel);
  }
  
  // ... rest of the method for regular streams and table scans ...
}
```

### 2. Connector Registration

In `VeloxBackend.cc`, the Kafka connector is registered alongside Hive and ValueStream connectors:

```cpp
// Register Kafka connector for streaming data from Kafka topics
velox::connector::registerConnector(
    std::make_shared<gluten::KafkaConnector>(kKafkaConnectorId, hiveConf));
```

### 3. Split Creation

When creating splits for Kafka streams, use `KafkaConnectorSplit`:

```cpp
auto kafkaSplit = std::make_shared<gluten::KafkaConnectorSplit>(
    kKafkaConnectorId,
    "localhost:9092",           // brokers
    "my-topic",                 // topic
    0,                          // partition
    0,                          // startOffset
    -1,                         // endOffset (-1 for latest)
    "my-consumer-group",        // groupId
    additionalProps             // additional Kafka properties
);
```

## Usage Example

### From Substrait Plan

When a Substrait ReadRel has `stream_kafka()` set to true, it will automatically:

1. Create a TableScanNode with `kKafkaConnectorId`
2. Extract schema from `base_schema`
3. Create appropriate column handles
4. Mark the split info as `TABLE_SCAN` type

### Creating Kafka Splits Programmatically

```cpp
#include "operators/reader/KafkaSplit.h"

// Create Kafka split
std::unordered_map<std::string, std::string> kafkaProps;
kafkaProps["auto.offset.reset"] = "earliest";
kafkaProps["enable.auto.commit"] = "false";

auto split = std::make_shared<gluten::KafkaConnectorSplit>(
    kKafkaConnectorId,
    "broker1:9092,broker2:9092",  // Kafka brokers
    "events-topic",                // Topic name
    0,                             // Partition number
    100,                           // Start offset
    1000,                          // End offset
    "gluten-consumer",             // Consumer group ID
    kafkaProps                     // Additional properties
);

// Add split to task
task->addSplit(planNodeId, std::move(split));
```

### Kafka Message Schema

The KafkaReader produces RowVectors with the following default schema:

- Column 0: `key` (VARBINARY) - Message key
- Column 1: `value` (VARBINARY) - Message payload
- Column 2: `topic` (VARCHAR) - Topic name
- Column 3: `partition` (INTEGER) - Partition number
- Column 4: `offset` (BIGINT) - Message offset
- Column 5: `timestamp` (BIGINT) - Message timestamp

You can customize the schema in the Substrait ReadRel's `base_schema`.

## Configuration

### Kafka Consumer Properties

Configure Kafka consumer through `KafkaReaderConfig`:

```cpp
config_.brokers = "localhost:9092";
config_.topic = "my-topic";
config_.partition = 0;
config_.startOffset = 0;
config_.endOffset = -1;  // Read until latest
config_.groupId = "my-group";
config_.maxPollRecords = 1000;
config_.pollTimeoutMs = 1000;
config_.additionalProps["security.protocol"] = "SASL_SSL";
```

### Velox Configuration

Add to your Velox configuration:

```cpp
const std::string kKafkaConnectorId = "kafka-stream";
```

## Implementation Details

### KafkaReader Operator

The `KafkaReader` is a `SourceOperator` that:

1. Initializes librdkafka consumer on first `getOutput()` call
2. Polls messages in batches (configurable via `maxPollRecords`)
3. Converts Kafka messages to Velox RowVectors
4. Manages offset commits
5. Handles partition EOF and errors

### Offset Management

- Offsets are committed after each batch
- Manual offset management (auto-commit disabled)
- Supports reading from specific offset ranges
- Handles partition EOF gracefully

### Error Handling

- Connection errors logged and operator marked as finished
- Message-level errors logged but don't stop processing
- Timeout errors handled gracefully (returns empty batch)

## Limitations and Future Work

1. **Single Partition**: Currently reads from one partition per split
2. **No Rebalancing**: Manual partition assignment (no consumer group rebalancing)
3. **Schema**: Fixed schema mapping (could be made configurable)
4. **Deserialization**: Messages returned as raw bytes (no built-in Avro/JSON support)

## Testing

To test the Kafka integration:

1. Start a Kafka broker
2. Create a test topic with data
3. Create a Substrait plan with `stream_kafka()` set
4. Execute the plan through Gluten

Example test setup:

```bash
# Start Kafka
docker run -d --name kafka -p 9092:9092 apache/kafka

# Create topic
kafka-topics --create --topic test-topic --bootstrap-server localhost:9092

# Produce test data
echo "test message" | kafka-console-producer --topic test-topic --bootstrap-server localhost:9092
```

## Dependencies

- librdkafka (C/C++ Kafka client library)
- Velox connector framework
- Substrait plan support

## See Also

- `KafkaReader.h` - Main reader implementation
- `KafkaSplit.h` - Split definition
- `KafkaConnector.h` - Connector interface
- `SubstraitToVeloxPlan.cc` - Plan conversion logic