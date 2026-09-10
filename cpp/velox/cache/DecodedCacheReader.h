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

#include <folly/container/F14Map.h>

#include "cache/DecodedCache.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"

namespace gluten {

/// Layers the decoded scan cache over another format's ReaderFactory. Every
/// connector (Hive, Iceberg, Delta) reaches its reader through
/// dwio::common::getReaderFactory(fileFormat), so wrapping the factory covers
/// all of them at once.
///
/// Anything this factory does not handle is delegated untouched, including
/// createFormatOptions -- forgetting to forward that would silently drop the
/// format's session options.
class DecodedCacheReaderFactory : public facebook::velox::dwio::common::ReaderFactory {
 public:
  DecodedCacheReaderFactory(std::shared_ptr<facebook::velox::dwio::common::ReaderFactory> delegate);

  std::unique_ptr<facebook::velox::dwio::common::Reader> createReader(
      std::unique_ptr<facebook::velox::dwio::common::BufferedInput> input,
      const facebook::velox::dwio::common::ReaderOptions& options) override;

  std::shared_ptr<facebook::velox::dwio::common::FormatSpecificOptions> createFormatOptions(
      const facebook::velox::config::ConfigBase& connectorConfig,
      const facebook::velox::config::ConfigBase& session) const override;

 private:
  const std::shared_ptr<facebook::velox::dwio::common::ReaderFactory> delegate_;
};

/// Replaces the factory currently registered for 'format' with one that layers
/// the decoded cache over it. No-op when nothing is registered for 'format'.
void registerDecodedCacheReaderFactory(facebook::velox::dwio::common::FileFormat format);

/// Hash of every reader setting that changes decoded values for identical file
/// bytes. This is the correctness core of the cache key: raw bytes are
/// self-describing, decoded values are not.
///
/// GLUTEN_DECODED_CACHE_CONTEXT_FIELDS -- when a Velox upgrade adds a reader
/// option that affects decoded values, it must be added here AND
/// DecodedCache::kFormatVersion must be bumped. There is no compiler help for
/// this; the mitigation is that the feature is off by default.
/// Currently covered:
///   ReaderOptions:    fileSchema, sessionTimezone, adjustTimestampToTimezone,
///                     fileColumnNamesReadAsLowerCase, columnMappingMode
///   RowReaderOptions: requestedType, timestampPrecision
///   ParquetReaderOptions: allowInt32Narrowing, nullStructIfAllFieldsMissing
uint64_t decodeContextHash(
    const facebook::velox::dwio::common::ReaderOptions& readerOptions,
    const facebook::velox::dwio::common::RowReaderOptions& rowReaderOptions);

} // namespace gluten
