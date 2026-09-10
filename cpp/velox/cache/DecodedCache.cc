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

#include "cache/DecodedCache.h"

#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <glog/logging.h>

namespace gluten {

using namespace facebook::velox;

namespace {

std::shared_ptr<DecodedCache>& instanceRef() {
  static std::shared_ptr<DecodedCache> instance;
  return instance;
}

/// Header of a split-index blob. Deliberately self-describing: a stale blob
/// left by an older build must be rejected, not misread.
struct SplitIndexHeader {
  uint32_t magic;
  uint32_t version;
  uint64_t numRanges;
};

constexpr uint32_t kSplitIndexMagic = 0x474c5349; // "GLSI"

} // namespace

std::string DecodedCacheStats::toString() const {
  return fmt::format(
      "DecodedCache: hitWindows {} hitBytes {} missWindows {} storedWindows {} storedBytes {} "
      "transcodeWallNanos {} splitsServedFromCache {} splitsTranscoded {} "
      "rejected[ghost {} entryTooLarge {} keyBudget {} pinBudget {}] ineligibleSplits {}",
      hitWindows.load(),
      hitBytes.load(),
      missWindows.load(),
      storedWindows.load(),
      storedBytes.load(),
      transcodeWallNanos.load(),
      splitsServedFromCache.load(),
      splitsTranscoded.load(),
      rejectedGhost.load(),
      rejectedEntryTooLarge.load(),
      rejectedKeyBudget.load(),
      rejectedPinBudget.load(),
      ineligibleSplits.load());
}

DecodedCache::DecodedCache(cache::AsyncDataCache* cache, DecodedCacheOptions options)
    : cache_(cache), options_(std::move(options)) {
  VELOX_CHECK_NOT_NULL(cache_, "DecodedCache requires an AsyncDataCache");
  VELOX_CHECK_GT(options_.windowRows, 0);
  VELOX_CHECK_GE(options_.admitMinTouches, 1);
}

DecodedCache* DecodedCache::getInstance() {
  return instanceRef().get();
}

void DecodedCache::setInstance(std::shared_ptr<DecodedCache> instance) {
  instanceRef() = std::move(instance);
}

void DecodedCache::releaseInstance() {
  instanceRef().reset();
}

void DecodedCache::registerFile(const std::string& path, int64_t fileSize, int64_t modificationTime) {
  std::lock_guard<std::mutex> l(mutex_);
  // Bounded by the same budget as the key namespaces: an executor that touches
  // millions of files must not accumulate identities forever.
  if (files_.size() >= static_cast<size_t>(options_.maxKeys)) {
    return;
  }
  files_[path] = FileIdentity{fileSize, modificationTime};
}

FileIdentity DecodedCache::fileIdentity(const std::string& path, int64_t fileSizeFallback) const {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = files_.find(path);
  if (it != files_.end()) {
    return it->second;
  }
  return FileIdentity{fileSizeFallback, -1};
}

std::optional<uint64_t> DecodedCache::keyId(const std::string& keyString) {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = leases_.find(keyString);
  if (it != leases_.end()) {
    return it->second.id();
  }
  if (static_cast<int64_t>(leases_.size()) >= options_.maxKeys) {
    stats_.rejectedKeyBudget.fetch_add(1);
    return std::nullopt;
  }
  StringIdLease lease(fileIds(), keyString);
  const auto id = lease.id();
  leases_.emplace(keyString, std::move(lease));
  return id;
}

std::optional<uint64_t> DecodedCache::columnKeyId(
    const std::string& filePath,
    const FileIdentity& identity,
    const std::string& columnName,
    uint64_t decodeContextHash) {
  return keyId(fmt::format(
      "gluten-decoded/v{}/col/{}@{}:{}/{}/{:x}",
      kFormatVersion,
      filePath,
      identity.modificationTime,
      identity.fileSize,
      columnName,
      decodeContextHash));
}

std::optional<uint64_t> DecodedCache::splitKeyId(
    const std::string& filePath,
    const FileIdentity& identity,
    uint64_t byteStart,
    uint64_t byteLength,
    uint64_t decodeContextHash) {
  return keyId(fmt::format(
      "gluten-decoded/v{}/split/{}@{}:{}/{}:{}/{:x}",
      kFormatVersion,
      filePath,
      identity.modificationTime,
      identity.fileSize,
      byteStart,
      byteLength,
      decodeContextHash));
}

bool DecodedCache::admit(uint64_t keyId, uint64_t offset) {
  if (options_.admitMinTouches <= 1) {
    return true;
  }
  const auto hash = folly::hash::hash_combine(keyId, offset);
  std::lock_guard<std::mutex> l(mutex_);
  // Clear-on-full rather than LRU: the ghost list only needs to remember
  // recent history, and a rare full reset costs one extra cold pass.
  if (ghost_.size() >= static_cast<size_t>(options_.maxKeys)) {
    ghost_.clear();
  }
  auto& touches = ghost_[hash];
  ++touches;
  if (touches < options_.admitMinTouches) {
    stats_.rejectedGhost.fetch_add(1);
    return false;
  }
  return true;
}

bool DecodedCache::store(cache::RawFileCacheKey key, const void* data, uint64_t size) {
  if (size > kMaxEntrySize) {
    stats_.rejectedEntryTooLarge.fetch_add(1);
    return false;
  }
  cache::CachePin pin;
  try {
    // No wait future: if another thread holds the entry exclusively it is
    // already populating the same bytes, so there is nothing to gain by
    // blocking a scan thread on it.
    pin = cache_->findOrCreate(key, size, /*contiguous=*/false, /*waitFuture=*/nullptr);
  } catch (const VeloxRuntimeError& e) {
    // Most likely kNoCacheSpace. Population is best effort.
    VLOG(1) << "DecodedCache::store failed to allocate: " << e.what();
    return false;
  }
  if (pin.empty()) {
    return false;
  }
  auto* entry = pin.entry();
  if (!entry->isExclusive()) {
    // Already populated by someone else.
    return true;
  }
  auto ranges = entry->dataRanges(size);
  const auto* source = reinterpret_cast<const char*>(data);
  uint64_t copied = 0;
  for (auto& range : ranges) {
    const auto n = std::min<uint64_t>(range.size(), size - copied);
    if (n == 0) {
      break;
    }
    ::memcpy(range.data(), source + copied, n);
    copied += n;
  }
  VELOX_CHECK_EQ(copied, size, "DecodedCache entry was allocated smaller than requested");
  entry->setExclusiveToShared();
  stats_.storedWindows.fetch_add(1);
  stats_.storedBytes.fetch_add(static_cast<int64_t>(size));
  return true;
}

std::optional<cache::CachePin> DecodedCache::pin(cache::RawFileCacheKey key) {
  auto pin = cache_->find(key, /*waitFuture=*/nullptr);
  if (!pin.has_value() || pin->empty()) {
    stats_.missWindows.fetch_add(1);
    return std::nullopt;
  }
  stats_.hitWindows.fetch_add(1);
  stats_.hitBytes.fetch_add(pin->entry()->size());
  return pin;
}

void DecodedCache::copyOut(cache::AsyncDataCacheEntry* entry, char* target) {
  const auto size = static_cast<uint64_t>(entry->size());
  auto ranges = entry->dataRanges(size);
  uint64_t copied = 0;
  for (auto& range : ranges) {
    const auto n = std::min<uint64_t>(range.size(), size - copied);
    if (n == 0) {
      break;
    }
    ::memcpy(target + copied, range.data(), n);
    copied += n;
  }
  VELOX_CHECK_EQ(copied, size, "Short read from a decoded cache entry");
}

void DecodedCache::makeEvictable(cache::RawFileCacheKey key) {
  cache_->makeEvictable(key);
}

std::string encodeSplitIndex(const std::vector<WindowRange>& ranges) {
  SplitIndexHeader header{kSplitIndexMagic, DecodedCache::kFormatVersion, ranges.size()};
  std::string out;
  out.resize(sizeof(header) + ranges.size() * sizeof(WindowRange));
  ::memcpy(out.data(), &header, sizeof(header));
  if (!ranges.empty()) {
    ::memcpy(out.data() + sizeof(header), ranges.data(), ranges.size() * sizeof(WindowRange));
  }
  return out;
}

bool decodeSplitIndex(const char* data, uint64_t size, std::vector<WindowRange>& ranges) {
  if (size < sizeof(SplitIndexHeader)) {
    return false;
  }
  SplitIndexHeader header{};
  ::memcpy(&header, data, sizeof(header));
  if (header.magic != kSplitIndexMagic || header.version != DecodedCache::kFormatVersion) {
    return false;
  }
  if (size != sizeof(header) + header.numRanges * sizeof(WindowRange)) {
    return false;
  }
  ranges.resize(header.numRanges);
  if (header.numRanges > 0) {
    ::memcpy(ranges.data(), data + sizeof(header), header.numRanges * sizeof(WindowRange));
  }
  for (const auto& range : ranges) {
    if (range.firstRow < 0 || range.numRows <= 0) {
      ranges.clear();
      return false;
    }
  }
  return true;
}

bool isCacheableType(const TypePtr& type) {
  if (type == nullptr) {
    return false;
  }
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::HUGEINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
      return true;
    default:
      // ROW/ARRAY/MAP and everything else falls back to the normal read path.
      return false;
  }
}

} // namespace gluten
