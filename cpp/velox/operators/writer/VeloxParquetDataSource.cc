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

#include "VeloxParquetDataSource.h"

#include <arrow/buffer.h>
#include <cstring>
#include <string>

#include "arrow/c/bridge.h"
#include "compute/VeloxRuntime.h"
#include "config/GlutenConfig.h"

#include "utils/ConfigExtractor.h"
#include "utils/VeloxArrowUtils.h"
#include "velox/common/compression/Compression.h"
#include "velox/core/QueryConfig.h"
#include "velox/core/QueryCtx.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/parquet/writer/arrow/util/Compression.h"

using namespace facebook;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::common;
using namespace facebook::velox::filesystems;

namespace gluten {
namespace {
const int32_t kGzipWindowBits4k = 12;
}

std::unique_ptr<facebook::velox::parquet::WriterOptions> VeloxParquetDataSource::makeParquetWriteOption(
    const std::unordered_map<std::string, std::string>& sparkConfs) {
  int64_t maxRowGroupBytes = 134217728; // 128MB
  int64_t maxRowGroupRows = 100000000; // 100M
  if (auto it = sparkConfs.find(kParquetBlockSize); it != sparkConfs.end()) {
    maxRowGroupBytes = static_cast<int64_t>(stoi(it->second));
  }
  if (auto it = sparkConfs.find(kParquetBlockRows); it != sparkConfs.end()) {
    maxRowGroupRows = static_cast<int64_t>(stoi(it->second));
  }
  auto writeOption = std::make_unique<facebook::velox::parquet::WriterOptions>();
  writeOption->parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds /*micro*/;
  auto compressionCodec = CompressionKind::CompressionKind_SNAPPY;
  if (auto it = sparkConfs.find(kParquetCompressionCodec); it != sparkConfs.end()) {
    auto compressionCodecStr = it->second;
    // spark support none, uncompressed, snappy, gzip, lzo, brotli, lz4, zstd.
    if (boost::iequals(compressionCodecStr, "snappy")) {
      compressionCodec = CompressionKind::CompressionKind_SNAPPY;
    } else if (boost::iequals(compressionCodecStr, "gzip")) {
      compressionCodec = CompressionKind::CompressionKind_GZIP;
      if (sparkConfs.find(kParquetGzipWindowSize) != sparkConfs.end()) {
        auto parquetGzipWindowSizeStr = sparkConfs.find(kParquetGzipWindowSize)->second;
        if (parquetGzipWindowSizeStr == kGzipWindowSize4k) {
          auto codecOptions = std::make_shared<facebook::velox::parquet::arrow::util::GZipCodecOptions>();
          codecOptions->window_bits = kGzipWindowBits4k;
          writeOption->codecOptions = std::move(codecOptions);
        }
      }
    } else if (boost::iequals(compressionCodecStr, "lzo")) {
      compressionCodec = CompressionKind::CompressionKind_LZO;
    } else if (boost::iequals(compressionCodecStr, "brotli")) {
      // please make sure `brotli` is enabled when compiling
      throw GlutenException("Gluten+velox does not support write parquet using brotli.");
    } else if (boost::iequals(compressionCodecStr, "lz4")) {
      compressionCodec = CompressionKind::CompressionKind_LZ4;
    } else if (boost::iequals(compressionCodecStr, "zstd")) {
      compressionCodec = CompressionKind::CompressionKind_ZSTD;
      if (auto it = sparkConfs.find(kParquetZSTDCompressionLevel); it != sparkConfs.end()) {
        auto compressionLevel = std::stoi(it->second);
        auto codecOptions = std::make_shared<facebook::velox::parquet::arrow::util::CodecOptions>();
        codecOptions->compression_level = compressionLevel;
        writeOption->codecOptions = std::move(codecOptions);
      }
    } else if (boost::iequals(compressionCodecStr, "uncompressed")) {
      compressionCodec = CompressionKind::CompressionKind_NONE;
    } else if (boost::iequals(compressionCodecStr, "none")) {
      compressionCodec = CompressionKind::CompressionKind_NONE;
    }
  }
  writeOption->compressionKind = compressionCodec;
  writeOption->flushPolicyFactory = [maxRowGroupRows, maxRowGroupBytes]() {
    return std::make_unique<velox::parquet::LambdaFlushPolicy>(
        maxRowGroupRows, maxRowGroupBytes, [&]() { return false; });
  };
  writeOption->parquetWriteTimestampTimeZone = getConfigValue(sparkConfs, kSessionTimezone, std::nullopt);
  if (auto it = sparkConfs.find(kParquetDataPageSize); it != sparkConfs.end()) {
    auto dataPageSize = std::stoi(it->second);
    writeOption->dataPageSize = dataPageSize;
  }
  if (auto it = sparkConfs.find(kParquetWriterVersion); it != sparkConfs.end()) {
    auto parquetVersion = it->second;
    if (boost::iequals(parquetVersion, "v2")) {
      writeOption->useParquetDataPageV2 = true;
    }
  }
  if (auto it = sparkConfs.find(kParquetEnableDictionary); it != sparkConfs.end()) {
    auto enableDictionary = it->second;
    if (boost::iequals(enableDictionary, "true")) {
      writeOption->enableDictionary = true;
    } else {
      writeOption->enableDictionary = false;
    }
  }
  return writeOption;
}

void VeloxParquetDataSource::initSink(const std::unordered_map<std::string, std::string>& /* sparkConfs */) {
  if (strncmp(filePath_.c_str(), "file:", 5) == 0) {
    sink_ = dwio::common::FileSink::create(filePath_, {.pool = pool_.get()});
  } else {
    throw std::runtime_error("The file path is not local when writing data with parquet format in velox runtime!");
  }
}

void VeloxParquetDataSource::init(const std::unordered_map<std::string, std::string>& sparkConfs) {
  initSink(sparkConfs);
  auto schema = gluten::fromArrowSchema(schema_);
  const auto writeOption = makeParquetWriteOption(sparkConfs);
  parquetWriter_ = std::make_unique<velox::parquet::Writer>(std::move(sink_), *writeOption, pool_, asRowType(schema));
}

void VeloxParquetDataSource::inspectSchema(struct ArrowSchema* out) {
  velox::dwio::common::ReaderOptions readerOptions(pool_.get());
  auto format = velox::dwio::common::FileFormat::PARQUET;
  readerOptions.setFileFormat(format);

  // Creates a file system: local, hdfs or s3.
  auto fs = velox::filesystems::getFileSystem(filePath_, nullptr);
  std::shared_ptr<velox::ReadFile> readFile{fs->openFileForRead(filePath_)};

  std::unique_ptr<velox::dwio::common::Reader> reader =
      velox::dwio::common::getReaderFactory(readerOptions.fileFormat())
          ->createReader(
              std::make_unique<velox::dwio::common::BufferedInput>(
                  std::make_shared<velox::dwio::common::ReadFileInputStream>(readFile), *pool_.get()),
              readerOptions);
  toArrowSchema(reader->rowType(), pool_.get(), out);
}

void VeloxParquetDataSource::close() {
  if (parquetWriter_) {
    parquetWriter_->close();
  }
}

void VeloxParquetDataSource::write(const std::shared_ptr<ColumnarBatch>& cb) {
  auto veloxBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  VELOX_DCHECK(veloxBatch != nullptr, "Write batch should be VeloxColumnarBatch");
  parquetWriter_->write(veloxBatch->getFlattenedRowVector());
}

} // namespace gluten
