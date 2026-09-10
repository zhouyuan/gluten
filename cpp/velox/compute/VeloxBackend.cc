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
#include <filesystem>

#include "VeloxBackend.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/task_queue/UnboundedBlockingQueue.h>

#include "compute/delta/DeltaConnector.h"
#include "operators/functions/RegistrationAllFunctions.h"
#include "operators/plannodes/RowVectorStream.h"
#include "utils/ConfigExtractor.h"

#ifdef GLUTEN_ENABLE_QAT
#include "utils/qat/QatCodec.h"
#endif
#ifdef GLUTEN_ENABLE_GPU
#include "cudf/GpuLock.h"
#include "operators/plannodes/CudfVectorStream.h"
#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveConnector.h"
#include "velox/experimental/cudf/exec/SparkAggregateFunctions.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/expression/SparkFunctions.h"
#endif

#include "cache/DecodedCacheReader.h"
#include "compute/VeloxRuntime.h"
#include "config/VeloxConfig.h"
#ifdef ENABLE_S3
#include "filesystem/GlutenS3FileSystem.h"
#endif
#include "jni/JniFileSystem.h"
#include "memory/GlutenBufferedInputBuilder.h"
#include "operators/functions/SparkExprToSubfieldFilterParser.h"
#include "operators/plannodes/RowVectorStream.h"
#include "shuffle/ArrowShuffleDictionaryWriter.h"
#include "udf/UdfLoader.h"
#include "utils/Exception.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/BufferedInputBuilder.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveDataSource.h"
#include "velox/connectors/hive/iceberg/IcebergConnector.h"
#include "velox/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/gcs/RegisterGcsFileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h" // @manual
#include "velox/dwio/orc/reader/OrcReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/serializers/PrestoSerializer.h"

DECLARE_bool(velox_exception_user_stacktrace_enabled);
DECLARE_int32(velox_memory_num_shared_leaf_pools);
DECLARE_bool(velox_memory_use_hugepages);
DECLARE_bool(velox_ssd_odirect);
DECLARE_bool(velox_memory_pool_capacity_transfer_across_tasks);
DECLARE_int32(cache_prefetch_min_pct);

using namespace facebook;

namespace gluten {

namespace {
MemoryManager* veloxMemoryManagerFactory(const std::string& kind, std::unique_ptr<AllocationListener> listener) {
  return new VeloxMemoryManager(kind, std::move(listener), *VeloxBackend::get()->getBackendConf());
}

void veloxMemoryManagerReleaser(MemoryManager* memoryManager) {
  delete memoryManager;
}

Runtime* veloxRuntimeFactory(
    const std::string& kind,
    MemoryManager* memoryManager,
    ThreadManager* threadManager,
    const std::unordered_map<std::string, std::string>& sessionConf) {
  auto* vmm = dynamic_cast<VeloxMemoryManager*>(memoryManager);
  GLUTEN_CHECK(vmm != nullptr, "Not a Velox memory manager");
  return new VeloxRuntime(kind, vmm, threadManager, sessionConf);
}

void veloxRuntimeReleaser(Runtime* runtime) {
  delete runtime;
}

class VeloxThreadManager : public ThreadManager {
 public:
  VeloxThreadManager(const std::string& kind, std::unique_ptr<ThreadInitializer> initializer)
      : ThreadManager(kind), initializer_(std::shared_ptr<ThreadInitializer>(std::move(initializer))) {}

  ThreadInitializer* getThreadInitializer() override {
    return initializer_.get();
  }

 private:
  std::shared_ptr<ThreadInitializer> initializer_;
};

ThreadManager* veloxThreadManagerFactory(const std::string& kind, std::unique_ptr<ThreadInitializer> initializer) {
  return new VeloxThreadManager(kind, std::move(initializer));
}

void veloxThreadManagerReleaser(ThreadManager* threadManager) {
  delete threadManager;
}

/// Removes the SSD tier's backing files. A tier that was never configured
/// leaves both strings empty, in which case there is nothing to remove.
void removeCacheFiles(const std::string& pathPrefix, const std::string& filePrefix) {
  if (pathPrefix.empty() || filePrefix.empty()) {
    return;
  }
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(pathPrefix, ec)) {
    if (entry.path().filename().string().find(filePrefix) != std::string::npos) {
      LOG(INFO) << "Removing cache file " << entry.path().filename().string();
      std::filesystem::remove(pathPrefix + "/" + entry.path().filename().string(), ec);
    }
  }
}

bool hasCudaDevice() {
#ifdef GLUTEN_ENABLE_GPU
  int count = 0;
  cudaError_t err = cudaGetDeviceCount(&count);
  return err == cudaSuccess && count > 0;
#else
  return false;
#endif
}
} // namespace

void VeloxBackend::init(
    std::unique_ptr<AllocationListener> listener,
    const std::unordered_map<std::string, std::string>& conf) {
  backendConf_ =
      std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>(conf));

  // Init glog and log level.
  if (!backendConf_->get<bool>(kDebugModeEnabled, false)) {
    FLAGS_v = backendConf_->get<uint32_t>(kGlogVerboseLevel, kGlogVerboseLevelDefault);
    FLAGS_minloglevel = backendConf_->get<uint32_t>(kGlogSeverityLevel, kGlogSeverityLevelDefault);
  } else {
    if (backendConf_->valueExists(kGlogVerboseLevel)) {
      FLAGS_v = backendConf_->get<uint32_t>(kGlogVerboseLevel, kGlogVerboseLevelDefault);
    } else {
      FLAGS_v = kGlogVerboseLevelMaximum;
    }
  }
  FLAGS_logtostderr = true;
  google::InitGoogleLogging("gluten");

  globalMemoryManager_ = std::make_unique<VeloxMemoryManager>(kVeloxBackendKind, std::move(listener), *backendConf_);

  // Register factories.
  MemoryManager::registerFactory(kVeloxBackendKind, veloxMemoryManagerFactory, veloxMemoryManagerReleaser);
  ThreadManager::registerFactory(kVeloxBackendKind, veloxThreadManagerFactory, veloxThreadManagerReleaser);
  Runtime::registerFactory(kVeloxBackendKind, veloxRuntimeFactory, veloxRuntimeReleaser);

  if (backendConf_->get<bool>(kDebugModeEnabled, false)) {
    LOG(INFO) << "VeloxBackend config:" << printConfig(backendConf_->rawConfigs());
  }

  // Allow growing buffer in another task through its memory pool.
  FLAGS_velox_memory_pool_capacity_transfer_across_tasks =
      backendConf_->get<bool>(kMemoryPoolCapacityTransferAcrossTasks, true);

  // Avoid creating too many shared leaf pools.
  FLAGS_velox_memory_num_shared_leaf_pools = 0;

  // Set velox_exception_user_stacktrace_enabled.
  FLAGS_velox_exception_user_stacktrace_enabled =
      backendConf_->get<bool>(kEnableUserExceptionStacktrace, kEnableUserExceptionStacktraceDefault);

  // Set velox_exception_system_stacktrace_enabled.
  FLAGS_velox_exception_system_stacktrace_enabled =
      backendConf_->get<bool>(kEnableSystemExceptionStacktrace, kEnableSystemExceptionStacktraceDefault);

  // Set velox_memory_use_hugepages.
  FLAGS_velox_memory_use_hugepages = backendConf_->get<bool>(kMemoryUseHugePages, kMemoryUseHugePagesDefault);

  // Set cache_prefetch_min_pct default as 0 to force all loads are prefetched in DirectBufferInput.
  FLAGS_cache_prefetch_min_pct = backendConf_->get<int>(kCachePrefetchMinPct, 0);

  hiveConnectorConfig_ = createHiveConnectorConfig(backendConf_);

  // Setup and register.
  velox::filesystems::registerLocalFileSystem();

#ifdef ENABLE_HDFS
  velox::filesystems::registerHdfsFileSystem();
#endif
#ifdef ENABLE_S3
  registerGlutenS3FileSystem();
#endif
#ifdef ENABLE_GCS
  velox::filesystems::registerGcsFileSystem();
#endif
#ifdef ENABLE_ABFS
  velox::filesystems::registerAbfsFileSystem();
  velox::filesystems::registerAzureClientProvider(*hiveConnectorConfig_);
#endif

#ifdef GLUTEN_ENABLE_GPU
  if (backendConf_->get<bool>(kCudfEnabled, kCudfEnabledDefault)) {
    if (hasCudaDevice()) {
      configureGpuTaskConcurrency(backendConf_->get<uint32_t>(kCudfConcurrentGpuTasks, kCudfConcurrentGpuTasksDefault));
      std::unordered_map<std::string, std::string> options = {
          {velox::cudf_velox::CudfConfig::kCudfEnabled, "true"},
          {velox::cudf_velox::CudfConfig::kCudfDebugEnabled, backendConf_->get(kDebugCudf, kDebugCudfDefault)},
          {velox::cudf_velox::CudfConfig::kCudfMemoryResource,
           backendConf_->get(kCudfMemoryResource, kCudfMemoryResourceDefault)},
          {velox::cudf_velox::CudfConfig::kCudfMemoryPercent,
           backendConf_->get(kCudfMemoryPercent, kCudfMemoryPercentDefault)},
          {velox::cudf_velox::CudfConfig::kCudfAllowCpuFallback,
           backendConf_->get(kCudfAllowCpuFallback, kCudfAllowCpuFallbackDefault)}};
      auto& cudfConfig = velox::cudf_velox::CudfConfig::getInstance();
      cudfConfig.initialize(std::move(options));
      velox::cudf_velox::registerCudf();
      velox::exec::Operator::registerOperator(std::make_unique<CudfVectorStreamOperatorTranslator>());
      velox::cudf_velox::registerSparkFunctions("");
      velox::cudf_velox::registerSparkAggregateFunctions("");
    } else {
      LOG(WARNING) << "No Cuda device found. Skip Cudf initialization.";
    }
  }
#endif

  const int32_t numTaskSlotsPerExecutor = [&]() {
    if (!backendConf_->valueExists(kNumTaskSlotsPerExecutor)) {
      LOG(WARNING) << kNumTaskSlotsPerExecutor << " is not set. Falling back to 1.";
      return 1;
    }
    return backendConf_->get<int32_t>(kNumTaskSlotsPerExecutor).value();
  }();
  GLUTEN_CHECK(
      numTaskSlotsPerExecutor >= 0,
      kNumTaskSlotsPerExecutor + " was set to negative number " + std::to_string(numTaskSlotsPerExecutor) +
          ", this should not happen.");

  const auto spillThreadNum = backendConf_->get<uint32_t>(kSpillThreadNum, kSpillThreadNumDefaultValue);
  if (spillThreadNum > 0) {
    spillExecutor_ = std::make_unique<folly::CPUThreadPoolExecutor>(spillThreadNum);
  }

  const auto ioThreads = backendConf_->get<int32_t>(kVeloxIOThreads, numTaskSlotsPerExecutor);
  GLUTEN_CHECK(
      ioThreads >= 0,
      kVeloxIOThreads + " was set to negative number " + std::to_string(ioThreads) + ", this should not happen.");
  if (ioThreads > 0) {
    ioExecutor_ =
        std::make_unique<folly::CPUThreadPoolExecutor>(ioThreads, folly::CPUThreadPoolExecutor::makeLifoSemQueue());
  }

  initJolFilesystem();

  velox::dwio::common::registerFileSinks();
  velox::parquet::registerParquetReaderFactory();
  velox::parquet::registerParquetWriterFactory();
  velox::orc::registerOrcReaderFactory();
  velox::exec::ExprToSubfieldFilterParser::registerParser(std::make_unique<SparkExprToSubfieldFilterParser>(
      backendConf_->get<bool>(kScanBloomFilterPushdownEnabled, kScanBloomFilterPushdownEnabledDefault)));
  velox::connector::hive::BufferedInputBuilder::registerBuilder(std::make_shared<GlutenBufferedInputBuilder>());

  // Register Velox functions
  registerAllFunctions();
  if (!facebook::velox::isRegisteredVectorSerde()) {
    // serde, for spill
    facebook::velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde("Presto")) {
    // RSS shuffle serde.
    facebook::velox::serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }

  initUdf();

  // Initialize Velox-side memory manager for current process. The memory manager
  // will be used during spill calls so we don't track it with Spark off-heap memory instead
  // we rely on overhead memory. If we track it with off-heap memory, recursive reservations from
  // Spark off-heap memory pool will be conducted to cause unexpected OOMs.
  auto sparkOverhead = backendConf_->get<int64_t>(kSparkOverheadMemory);
  int64_t memoryManagerCapacity;
  if (sparkOverhead.has_value()) {
    // Get configurable ratio for Velox global memory manager capacity
    auto capacityRatio = backendConf_->get<double>(kMemoryManagerCapacityRatio);
    double ratio = capacityRatio.has_value() ? capacityRatio.value() : kMemoryManagerCapacityRatioDefault;

    if (ratio <= 0.0 || ratio > 1.0) {
      LOG(WARNING) << "Invalid memory manager capacity ratio: " << ratio
                   << ". Using default: " << kMemoryManagerCapacityRatioDefault;
      ratio = kMemoryManagerCapacityRatioDefault;
    }

    memoryManagerCapacity = static_cast<int64_t>(sparkOverhead.value() * ratio);
    LOG(INFO) << "Using memory manager capacity ratio: " << ratio << " (overhead: " << sparkOverhead.value()
              << ", capacity: " << memoryManagerCapacity << ")";
  } else {
    memoryManagerCapacity = facebook::velox::memory::kMaxMemory;
  }
  LOG(INFO) << "Setting global Velox memory manager with capacity: " << memoryManagerCapacity;
  facebook::velox::memory::MemoryManager::Options options;
  options.allocatorCapacity = memoryManagerCapacity;
  facebook::velox::memory::initializeMemoryManager(options);

  // local cache persistent relies on the cache pool from root memory pool so we need to init this
  // after the memory manager instanced
  initCache();
  // Independent of initCache: the decoded cache can run with its own store
  // when the raw byte cache is disabled.
  initDecodedCache();

  registerShuffleDictionaryWriterFactory([](MemoryManager* memoryManager, arrow::util::Codec* codec) {
    return std::make_unique<ArrowShuffleDictionaryWriter>(memoryManager, codec);
  });
}

facebook::velox::cache::AsyncDataCache* VeloxBackend::getAsyncDataCache() const {
  return asyncDataCache_.get();
}

ReaderThreadPool* VeloxBackend::getReaderThreadPool() {
  static std::once_flag readerThreadPoolInit;
  std::call_once(readerThreadPoolInit, [this] {
    const auto numThreads =
        backendConf_->get<int32_t>(kGpuAsyncShuffleReaderThreads, kGpuAsyncShuffleReaderThreadsDefault);
    readerThreadPool_ = std::make_unique<ReaderThreadPool>(numThreads);
  });
  return readerThreadPool_.get();
}

// JNI-or-local filesystem, for spilling-to-heap if we have extra JVM heap spaces
void VeloxBackend::initJolFilesystem() {
  int64_t maxSpillFileSize = backendConf_->get<int64_t>(kMaxSpillFileSize, kMaxSpillFileSizeDefault);

  // FIXME It's known that if spill compression is disabled, the actual spill file size may
  //   in crease beyond this limit a little (maximum 64 rows which is by default
  //   one compression page)
  registerJolFileSystem(maxSpillFileSize);
}

VeloxBackend::SsdCacheHandle VeloxBackend::initSsdCache(
    uint64_t ssdSize,
    const std::string& pathPrefix,
    int32_t shards,
    int32_t ioThreads,
    const std::string& filePrefixTag,
    bool allowCheckpoint) {
  FLAGS_velox_ssd_odirect = backendConf_->get<bool>(kVeloxSsdODirectEnabled, false);
  uint64_t ssdCheckpointIntervalSize =
      allowCheckpoint ? backendConf_->get<uint64_t>(kVeloxSsdCheckpointIntervalBytes, 0) : 0;
  bool disableFileCow = backendConf_->get<bool>(kVeloxSsdDisableFileCow, false);
  bool checksumEnabled = backendConf_->get<bool>(kVeloxSsdCheckSumEnabled, false);
  bool checksumReadVerificationEnabled = backendConf_->get<bool>(kVeloxSsdCheckSumReadVerificationEnabled, false);

  SsdCacheHandle handle;
  handle.pathPrefix = pathPrefix;
  // A fresh uuid per tier, so two tiers sharing a directory cannot write each
  // other's files.
  handle.filePrefix = filePrefixTag + getCacheFilePrefix();
  const std::string ssdCachePath = pathPrefix + "/" + handle.filePrefix;
  handle.executor = std::make_unique<folly::IOThreadPoolExecutor>(ioThreads);
  const cache::SsdCache::Config config(
      ssdCachePath,
      ssdSize,
      shards,
      handle.executor.get(),
      ssdCheckpointIntervalSize,
      disableFileCow,
      checksumEnabled,
      checksumReadVerificationEnabled);
  handle.cache = std::make_unique<velox::cache::SsdCache>(config);
  std::error_code ec;
  const std::filesystem::space_info si = std::filesystem::space(pathPrefix, ec);
  if (si.available < ssdSize) {
    VELOX_FAIL(
        "not enough space for ssd cache in " + ssdCachePath + " cache size: " + std::to_string(ssdSize) +
        "free space: " + std::to_string(si.available));
  }
  LOG(INFO) << "Initializing SSD cache at " << ssdCachePath << " with: " << config.toString();
  return handle;
}

void VeloxBackend::initCache() {
  if (backendConf_->get<bool>(kVeloxCacheEnabled, false)) {
    uint64_t memCacheSize = backendConf_->get<uint64_t>(kVeloxMemCacheSize, kVeloxMemCacheSizeDefault);
    uint64_t ssdCacheSize = backendConf_->get<uint64_t>(kVeloxSsdCacheSize, kVeloxSsdCacheSizeDefault);

    velox::memory::MmapAllocator::Options options;
    options.capacity = memCacheSize;
    cacheAllocator_ = std::make_shared<velox::memory::MmapAllocator>(options);
    if (ssdCacheSize == 0) {
      LOG(INFO) << "AsyncDataCache will do memory caching only as ssd cache size is 0";
      // TODO: this is not tracked by Spark.
      asyncDataCache_ = velox::cache::AsyncDataCache::create(cacheAllocator_.get());
    } else {
      // TODO: this is not tracked by Spark.
      auto handle = initSsdCache(
          ssdCacheSize,
          backendConf_->get<std::string>(kVeloxSsdCachePath, kVeloxSsdCachePathDefault),
          backendConf_->get<int32_t>(kVeloxSsdCacheShards, kVeloxSsdCacheShardsDefault),
          backendConf_->get<int32_t>(kVeloxSsdCacheIOThreads, kVeloxSsdCacheIOThreadsDefault),
          /*filePrefixTag=*/"",
          /*allowCheckpoint=*/true);
      ssdCacheExecutor_ = std::move(handle.executor);
      cachePathPrefix_ = handle.pathPrefix;
      cacheFilePrefix_ = handle.filePrefix;
      asyncDataCache_ = velox::cache::AsyncDataCache::create(cacheAllocator_.get(), std::move(handle.cache));
    }

    VELOX_CHECK_NOT_NULL(dynamic_cast<velox::cache::AsyncDataCache*>(asyncDataCache_.get()));
    LOG(INFO) << "AsyncDataCache is ready";
  }
}

void VeloxBackend::initDecodedCache() {
  if (!backendConf_->get<bool>(kVeloxDecodedCacheEnabled, kVeloxDecodedCacheEnabledDefault)) {
    return;
  }

  // The decoded cache needs an AsyncDataCache to hold its windows, but it does
  // not need the *raw* byte cache. Two arrangements:
  //
  //  - Raw cache on: share its instance. Decoded and raw entries then compete
  //    in one budget by LRU, which is what you want since a decoded window
  //    makes its raw counterpart nearly worthless. The SSD tier comes along.
  //  - Raw cache off: build a private memory-only store. This is the
  //    'decoded only' configuration, and it is the more sensible one for a
  //    workload that always reads the same columns.
  //
  // A second AsyncDataCache is safe: create() only calls
  // allocator->registerCache() on its own allocator and never touches the
  // static AsyncDataCache::setInstance(), which Gluten does not use -- it
  // passes the cache to QueryCtx explicitly.
  velox::cache::AsyncDataCache* store = dynamic_cast<velox::cache::AsyncDataCache*>(asyncDataCache_.get());
  if (store == nullptr) {
    const auto memSize = backendConf_->get<uint64_t>(kVeloxDecodedCacheMemSize, kVeloxDecodedCacheMemSizeDefault);
    GLUTEN_CHECK(memSize > 0, "decodedCacheMemSize must be positive when the Velox cache is disabled");
    velox::memory::MmapAllocator::Options allocatorOptions;
    allocatorOptions.capacity = memSize;
    decodedCacheAllocator_ = std::make_shared<velox::memory::MmapAllocator>(allocatorOptions);

    // Optional SSD tier for the private store. Worth more here than for raw
    // bytes: an SSD hit still skips decompression and decoding, not just IO.
    //
    // Checkpointing is deliberately forced off for this tier, so it is a
    // within-process capacity extension and never outlives the executor.
    // SsdFile checkpoints would persist entries and, because they store file
    // *names* and rebuild the id mapping on recovery, deterministic decoded
    // keys would be found again after a restart. But the payload is a
    // PrestoVectorSerde blob -- a wire/spill format with no on-disk stability
    // contract -- and the key has no notion of which Velox produced it. After
    // a Gluten/Velox upgrade a recovered entry could deserialize under a
    // different serde. Cross-restart persistence needs a payload format we
    // version ourselves, which is what the phase-2 encoding would provide.
    const auto ssdSize = backendConf_->get<uint64_t>(kVeloxDecodedCacheSsdSize, kVeloxDecodedCacheSsdSizeDefault);
    // TODO: this is not tracked by Spark, same as the raw cache arena above.
    if (ssdSize == 0) {
      decodedCacheStore_ = velox::cache::AsyncDataCache::create(decodedCacheAllocator_.get());
      LOG(INFO) << "Decoded scan cache owns a private memory-only store of " << memSize << " bytes";
    } else {
      auto handle = initSsdCache(
          ssdSize,
          backendConf_->get<std::string>(kVeloxDecodedCacheSsdPath, kVeloxSsdCachePathDefault),
          backendConf_->get<int32_t>(kVeloxDecodedCacheSsdShards, kVeloxSsdCacheShardsDefault),
          backendConf_->get<int32_t>(kVeloxDecodedCacheSsdIOThreads, kVeloxSsdCacheIOThreadsDefault),
          /*filePrefixTag=*/"decoded.",
          /*allowCheckpoint=*/false);
      decodedSsdCacheExecutor_ = std::move(handle.executor);
      decodedCachePathPrefix_ = handle.pathPrefix;
      decodedCacheFilePrefix_ = handle.filePrefix;
      decodedCacheStore_ = velox::cache::AsyncDataCache::create(decodedCacheAllocator_.get(), std::move(handle.cache));
      LOG(INFO) << "Decoded scan cache owns a private store of " << memSize << " bytes in memory and " << ssdSize
                << " bytes on SSD";
    }
    store = decodedCacheStore_.get();
  } else {
    LOG(INFO) << "Decoded scan cache shares the Velox cache instance";
  }
  // Note the private store is deliberately never handed to QueryCtx: doing so
  // would let the ordinary read path fill it with raw bytes.

  DecodedCacheOptions options;
  options.windowRows = backendConf_->get<int32_t>(kVeloxDecodedCacheWindowRows, kVeloxDecodedCacheWindowRowsDefault);
  options.admitMinTouches =
      backendConf_->get<int32_t>(kVeloxDecodedCacheAdmitMinTouches, kVeloxDecodedCacheAdmitMinTouchesDefault);
  options.serveFilteredReads =
      backendConf_->get<bool>(kVeloxDecodedCacheServeFilteredReads, kVeloxDecodedCacheServeFilteredReadsDefault);
  options.maxPinnedBytesPerSplit = backendConf_->get<int64_t>(
      kVeloxDecodedCacheMaxPinnedBytesPerSplit, kVeloxDecodedCacheMaxPinnedBytesPerSplitDefault);
  options.maxKeys = backendConf_->get<int64_t>(kVeloxDecodedCacheMaxKeys, kVeloxDecodedCacheMaxKeysDefault);
  GLUTEN_CHECK(options.windowRows > 0, "decodedCacheWindowRows must be positive");
  GLUTEN_CHECK(options.admitMinTouches >= 1, "decodedCacheAdmitMinTouches must be at least 1");

  DecodedCache::setInstance(std::make_shared<DecodedCache>(store, std::move(options)));
  // Layer the decoded cache over whatever is registered for Parquet. Only
  // Parquet is covered: the phase-1 cached representation is produced from a
  // Velox RowVector, but the eligibility rules and the row-range bookkeeping
  // have only been reasoned about against the Parquet reader's row-group
  // semantics.
  registerDecodedCacheReaderFactory(velox::dwio::common::FileFormat::PARQUET);
  LOG(INFO) << "Decoded scan cache is enabled: windowRows=" << options.windowRows
            << " admitMinTouches=" << options.admitMinTouches << " serveFilteredReads=" << options.serveFilteredReads;
}

std::shared_ptr<facebook::velox::connector::Connector> VeloxBackend::createHiveConnector(
    const std::string& connectorId,
    folly::Executor* ioExecutor) const {
  return std::make_shared<velox::connector::hive::HiveConnector>(connectorId, hiveConnectorConfig_, ioExecutor);
}

std::shared_ptr<facebook::velox::connector::Connector> VeloxBackend::createDeltaConnector(
    const std::string& connectorId,
    folly::Executor* ioExecutor) const {
  return std::make_shared<delta::DeltaConnector>(connectorId, hiveConnectorConfig_, ioExecutor);
}

std::shared_ptr<facebook::velox::connector::Connector> VeloxBackend::createIcebergConnector(
    const std::string& connectorId,
    folly::Executor* ioExecutor) const {
  return std::make_shared<velox::connector::hive::iceberg::IcebergConnector>(
      connectorId, hiveConnectorConfig_, ioExecutor);
}

std::shared_ptr<facebook::velox::connector::Connector> VeloxBackend::createValueStreamConnector(
    const std::string& connectorId,
    bool dynamicFilterEnabled) const {
  return std::make_shared<ValueStreamConnector>(connectorId, hiveConnectorConfig_, dynamicFilterEnabled);
}

#ifdef GLUTEN_ENABLE_GPU
std::shared_ptr<facebook::velox::connector::Connector> VeloxBackend::createCudfHiveConnector(
    const std::string& connectorId,
    folly::Executor* ioExecutor) const {
  facebook::velox::cudf_velox::connector::hive::CudfHiveConnectorFactory factory;
  return factory.newConnector(connectorId, hiveConnectorConfig_, ioExecutor);
}
#endif

void VeloxBackend::initUdf() {
  auto got = backendConf_->get<std::string>(kVeloxUdfLibraryPaths, "");
  if (!got.empty()) {
    auto udfLoader = UdfLoader::getInstance();
    udfLoader->loadUdfLibraries(got);
    udfLoader->registerUdf();
  }
}

std::unique_ptr<VeloxBackend> VeloxBackend::instance_ = nullptr;

void VeloxBackend::create(
    std::unique_ptr<AllocationListener> listener,
    const std::unordered_map<std::string, std::string>& conf) {
  instance_ = std::unique_ptr<VeloxBackend>(new VeloxBackend(std::move(listener), conf));
}

VeloxBackend* VeloxBackend::get() {
  if (!instance_) {
    LOG(WARNING) << "VeloxBackend instance is null, please invoke VeloxBackend#create before use.";
    throw GlutenException("VeloxBackend instance is null.");
  }
  return instance_.get();
}

void VeloxBackend::tearDown() {
#ifdef ENABLE_HDFS
  for (const auto& [_, filesystem] : facebook::velox::filesystems::registeredFilesystems) {
    filesystem->close();
  }
#endif
#ifdef ENABLE_S3
  finalizeGlutenS3FileSystem();
#endif

  // Destruct IOThreadPoolExecutor will join all threads.
  // On threads exit, thread local variables can be constructed with referencing global variables.
  // So, we need to destruct IOThreadPoolExecutor and stop the threads before global variables get destructed.
  executor_.reset();
  spillExecutor_.reset();
  ioExecutor_.reset();
  ssdCacheExecutor_.reset();
  decodedSsdCacheExecutor_.reset();
  globalMemoryManager_.reset();

  // Release the decoded cache first, whichever store it borrows: it may still
  // hold pins, and those must not outlive the cache they point into.
  if (auto* decodedCache = DecodedCache::getInstance()) {
    LOG(INFO) << decodedCache->stats().toString();
  }
  DecodedCache::releaseInstance();
  if (decodedCacheStore_ != nullptr) {
    LOG(INFO) << decodedCacheStore_->toString();
    decodedCacheStore_->shutdown();
    decodedCacheStore_.reset();
    decodedCacheAllocator_.reset();
    removeCacheFiles(decodedCachePathPrefix_, decodedCacheFilePrefix_);
  }

  // dump cache stats on exit if enabled
  if (dynamic_cast<facebook::velox::cache::AsyncDataCache*>(asyncDataCache_.get())) {
    LOG(INFO) << asyncDataCache_->toString();
    removeCacheFiles(cachePathPrefix_, cacheFilePrefix_);
    asyncDataCache_->shutdown();
  }
}

} // namespace gluten
