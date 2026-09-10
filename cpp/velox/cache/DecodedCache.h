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

#include <atomic>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <folly/container/F14Map.h>

#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"
#include "velox/vector/ComplexVector.h"

namespace gluten {

/// Tunables of the decoded scan cache. See docs/developers/VeloxDecodedCache.md.
struct DecodedCacheOptions {
  /// Rows per cached window. Windows are aligned to the start of the containing
  /// row group so that a window never spans two row groups.
  int32_t windowRows{65536};

  /// A chunk is transcoded only after its key has been seen this many times.
  /// 1 admits on first touch. Protects one-shot scans from paying the
  /// transcode cost with nobody to amortize it.
  int32_t admitMinTouches{2};

  /// When false, only reads without pushed-down filters are served from the
  /// cache. Cached reads materialize all rows of a window before filtering, so
  /// a very selective filter over wide projections can cost more CPU than the
  /// selective reader over a warm raw cache; this is the escape hatch.
  bool serveFilteredReads{true};

  /// Upper bound on cache bytes pinned on behalf of a single split. A split is
  /// served from the cache only if all of its windows fit; pinning costs no
  /// extra memory but prevents eviction, so this bounds cache starvation.
  int64_t maxPinnedBytesPerSplit{512LL << 20};

  /// Upper bound on distinct cache-key namespaces. Each namespace holds a
  /// StringIdLease for the lifetime of the process (releasing it would let the
  /// id be recycled and alias a different file), so this is a hard memory
  /// bound, not a hint. New keys are refused once it is reached.
  int64_t maxKeys{1 << 20};
};

struct DecodedCacheStats {
  std::atomic<int64_t> hitWindows{0};
  std::atomic<int64_t> hitBytes{0};
  std::atomic<int64_t> missWindows{0};
  std::atomic<int64_t> storedWindows{0};
  std::atomic<int64_t> storedBytes{0};
  std::atomic<int64_t> transcodeWallNanos{0};
  std::atomic<int64_t> splitsServedFromCache{0};
  std::atomic<int64_t> splitsTranscoded{0};
  std::atomic<int64_t> rejectedGhost{0};
  std::atomic<int64_t> rejectedEntryTooLarge{0};
  std::atomic<int64_t> rejectedKeyBudget{0};
  std::atomic<int64_t> rejectedPinBudget{0};
  std::atomic<int64_t> ineligibleSplits{0};

  std::string toString() const;
};

/// Identity of a data file. Part of every cache key: decoded chunks of a
/// rewritten file must not be served for the new file.
struct FileIdentity {
  int64_t fileSize{0};
  /// -1 when the query plan did not carry a modification time. Keys built from
  /// such an identity fall back to size-only freshness, which cannot detect a
  /// same-length rewrite.
  int64_t modificationTime{-1};
};

/// One cached window: a contiguous run of rows of one leaf column, unfiltered
/// and unpruned, never spanning a row group.
struct WindowRange {
  /// File-absolute row number of the first row, matching
  /// dwio::common::RowReader::nextRowNumber().
  int64_t firstRow{0};
  int64_t numRows{0};
};

/// Process-wide store of decoded column windows, layered on Velox's
/// AsyncDataCache so that the memory tier, SSD tier, eviction and
/// checkpoint/restore are inherited rather than reimplemented.
///
/// Decoded entries live in the same budget as raw entries and compete by LRU.
class DecodedCache {
 public:
  /// Bumping this invalidates every previously cached window. Bump it whenever
  /// the on-cache layout, or the set of fields covered by the decode-context
  /// hash, changes.
  static constexpr int32_t kFormatVersion = 1;

  /// SsdFile refuses entries larger than 1 << SsdFile::kSizeBits, so a window
  /// serialized above this cannot reach the SSD tier. Rather than keep a
  /// memory-only entry with different semantics, oversized windows are not
  /// cached at all.
  static constexpr uint64_t kMaxEntrySize = 1UL << 23;

  DecodedCache(facebook::velox::cache::AsyncDataCache* cache, DecodedCacheOptions options);

  /// Set once during backend initialization. Null when the feature is off.
  static DecodedCache* getInstance();
  static void setInstance(std::shared_ptr<DecodedCache> instance);
  static void releaseInstance();

  const DecodedCacheOptions& options() const {
    return options_;
  }

  DecodedCacheStats& stats() {
    return stats_;
  }

  facebook::velox::cache::AsyncDataCache* cache() const {
    return cache_;
  }

  /// Records the identity of a file as it is turned into splits. Called from
  /// the plan converter because the modification time carried by the Substrait
  /// plan is dropped before the reader sees it: FileHandle keeps only
  /// {file, uuid, groupId}.
  void registerFile(const std::string& path, int64_t fileSize, int64_t modificationTime);

  /// Returns the registered identity, or a size-only identity when the file was
  /// never registered (for example a split produced by a path this build does
  /// not cover).
  FileIdentity fileIdentity(const std::string& path, int64_t fileSizeFallback) const;

  /// Key namespace for the windows of one leaf column. Returns nullopt when the
  /// key budget is exhausted.
  std::optional<uint64_t> columnKeyId(
      const std::string& filePath,
      const FileIdentity& identity,
      const std::string& columnName,
      uint64_t decodeContextHash);

  /// Key namespace for the row-range index of one split.
  std::optional<uint64_t> splitKeyId(
      const std::string& filePath,
      const FileIdentity& identity,
      uint64_t byteStart,
      uint64_t byteLength,
      uint64_t decodeContextHash);

  /// Ghost-list admission. Returns true when this key has been seen at least
  /// 'admitMinTouches' times, counting this call.
  bool admit(uint64_t keyId, uint64_t offset);

  /// Writes 'size' bytes under 'key'. Returns false when the entry is too
  /// large, when another thread is already populating it, or when the cache has
  /// no space. Population is best effort: a false return is never an error.
  bool store(facebook::velox::cache::RawFileCacheKey key, const void* data, uint64_t size);

  /// Returns a shared pin on an existing entry, or nullopt on a miss. Holding
  /// the pin prevents eviction, which is what makes a cache-served split safe
  /// against eviction mid-read.
  std::optional<facebook::velox::cache::CachePin> pin(facebook::velox::cache::RawFileCacheKey key);

  /// Copies the whole entry into 'target', which must have room for
  /// entry->size() bytes. Uses dataRanges() so it is correct for entries backed
  /// by tiny data, a contiguous region, or non-contiguous page runs.
  static void copyOut(facebook::velox::cache::AsyncDataCacheEntry* entry, char* target);

  /// Marks an entry as preferentially evictable. Applied to windows of columns
  /// that carry no pushed-down filter, so the memory tier fills with the
  /// windows that predicates actually touch and the rest drains to SSD.
  void makeEvictable(facebook::velox::cache::RawFileCacheKey key);

 private:
  std::optional<uint64_t> keyId(const std::string& keyString);

  facebook::velox::cache::AsyncDataCache* const cache_;
  const DecodedCacheOptions options_;
  DecodedCacheStats stats_;

  mutable std::mutex mutex_;
  /// Leases must outlive the entries keyed by their ids, otherwise a recycled
  /// id would alias a different file. Held for the process lifetime, bounded by
  /// options_.maxKeys.
  folly::F14FastMap<std::string, facebook::velox::StringIdLease> leases_;
  folly::F14FastMap<uint64_t, int32_t> ghost_;
  folly::F14FastMap<std::string, FileIdentity> files_;
};

/// Encodes the row ranges a split covers. Written only from an unfiltered pass,
/// where no row group is excluded by statistics, so the encoded ranges are
/// independent of the filters of the query that happened to populate them.
std::string encodeSplitIndex(const std::vector<WindowRange>& ranges);
bool decodeSplitIndex(const char* data, uint64_t size, std::vector<WindowRange>& ranges);

/// True for the types the phase-1 cache handles. Nested types, and anything
/// needing repetition-level reconstruction, are left to the normal read path.
bool isCacheableType(const facebook::velox::TypePtr& type);

} // namespace gluten
