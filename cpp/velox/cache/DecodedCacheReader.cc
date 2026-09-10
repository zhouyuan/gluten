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

#include "cache/DecodedCacheReader.h"

#include <algorithm>

#include <folly/hash/Hash.h>
#include <glog/logging.h>

#include "cache/DecodedWindowFormat.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/vector/FlatVector.h"

namespace gluten {

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;

namespace {

/// One leaf column backed by the cache.
struct CachedColumn {
  std::string name;
  TypePtr type;
  uint64_t keyId{0};
  /// True when the query pushes a filter on this column. Windows of columns
  /// with no filter are marked evictable after use so the memory tier fills
  /// with the windows predicates actually touch.
  bool hasFilter{false};
};

/// Row reader that serves a split from decoded windows, or reads it
/// unfiltered and transcodes it into windows, depending on the mode chosen
/// when it was created.
///
/// Both modes apply the query's filters and mutations through
/// RowReader::projectColumns, so no filter evaluation is reimplemented here.
class DecodedCacheRowReader : public RowReader {
 public:
  enum class Mode { kServe, kPopulate };

  /// Serving constructor. 'pins' must cover every window of every column for
  /// the whole split; holding them is what makes the split immune to eviction
  /// part way through the read.
  DecodedCacheRowReader(
      DecodedCache* cache,
      const RowReaderOptions& options,
      memory::MemoryPool* pool,
      std::vector<CachedColumn> columns,
      std::vector<WindowRange> ranges,
      std::vector<folly::F14FastMap<int64_t, cache::CachePin>> pins)
      : mode_(Mode::kServe),
        cache_(cache),
        scanSpec_(options.scanSpec()),
        pool_(pool),
        columns_(std::move(columns)),
        ranges_(std::move(ranges)),
        pins_(std::move(pins)),
        windowRows_(cache->options().windowRows) {
    buildInputRowType();
  }

  /// Populating constructor. 'delegate' must have been created with a
  /// filterless scan spec and no metadata filter, so the rows it produces are
  /// reusable by queries with any predicate.
  DecodedCacheRowReader(
      DecodedCache* cache,
      const RowReaderOptions& options,
      memory::MemoryPool* pool,
      std::vector<CachedColumn> columns,
      std::unique_ptr<RowReader> delegate,
      uint64_t splitKeyId)
      : mode_(Mode::kPopulate),
        cache_(cache),
        scanSpec_(options.scanSpec()),
        pool_(pool),
        columns_(std::move(columns)),
        delegate_(std::move(delegate)),
        splitKeyId_(splitKeyId),
        windowRows_(cache->options().windowRows) {
    buildInputRowType();
    staging_.resize(columns_.size());
  }

  uint64_t next(uint64_t size, VectorPtr& result, const Mutation* mutation) override {
    return mode_ == Mode::kServe ? nextFromCache(size, result, mutation) : nextAndPopulate(size, result, mutation);
  }

  int64_t nextRowNumber() override {
    if (mode_ == Mode::kPopulate) {
      return delegate_->nextRowNumber();
    }
    while (rangeIdx_ < ranges_.size() && rowInRange_ >= ranges_[rangeIdx_].numRows) {
      ++rangeIdx_;
      rowInRange_ = 0;
    }
    if (rangeIdx_ >= ranges_.size()) {
      return kAtEnd;
    }
    return ranges_[rangeIdx_].firstRow + rowInRange_;
  }

  int64_t nextReadSize(uint64_t size) override {
    VELOX_CHECK_GT(size, 0);
    if (mode_ == Mode::kPopulate) {
      return delegate_->nextReadSize(size);
    }
    if (nextRowNumber() == kAtEnd) {
      return kAtEnd;
    }
    const auto& range = ranges_[rangeIdx_];
    // Clamp to the end of the range, mirroring how Velox never reads across a
    // row group, and to the end of the window so a read never spans two
    // cache entries.
    const int64_t inRange = range.numRows - rowInRange_;
    const int64_t inWindow = windowRows_ - (rowInRange_ % windowRows_);
    return std::min<int64_t>({static_cast<int64_t>(size), inRange, inWindow});
  }

  void updateRuntimeStats(RuntimeStats& stats) const override {
    if (mode_ == Mode::kPopulate) {
      delegate_->updateRuntimeStats(stats);
      return;
    }
    stats.processedStrides += static_cast<int64_t>(ranges_.size());
  }

  void resetFilterCaches() override {
    if (mode_ == Mode::kPopulate) {
      delegate_->resetFilterCaches();
    }
    // Serving applies filters through ScanSpec::applyFilter, which keeps no
    // per-file cached state.
  }

  std::optional<size_t> estimatedRowSize() const override {
    return mode_ == Mode::kPopulate ? delegate_->estimatedRowSize() : std::nullopt;
  }

  bool allPrefetchIssued() const override {
    // Serving issues no IO at all, so every read the caller can make is ready.
    return mode_ == Mode::kServe ? true : delegate_->allPrefetchIssued();
  }

  std::optional<std::vector<PrefetchUnit>> prefetchUnits() override {
    // Serving has nothing to prefetch; nullopt is how a reader says so.
    return mode_ == Mode::kServe ? std::nullopt : delegate_->prefetchUnits();
  }

  uint32_t currentStripe() const override {
    if (mode_ == Mode::kPopulate) {
      return delegate_->currentStripe();
    }
    return static_cast<uint32_t>(rangeIdx_);
  }

 private:
  void buildInputRowType() {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    names.reserve(columns_.size());
    types.reserve(columns_.size());
    for (const auto& column : columns_) {
      names.push_back(column.name);
      types.push_back(column.type);
    }
    inputRowType_ = ROW(std::move(names), std::move(types));
  }

  int64_t windowFirstRow(int64_t rangeFirstRow, int64_t rowInRange) const {
    return rangeFirstRow + (rowInRange / windowRows_) * windowRows_;
  }

  uint64_t nextFromCache(uint64_t size, VectorPtr& result, const Mutation* mutation) {
    const auto rowsToRead = nextReadSize(size);
    if (rowsToRead == kAtEnd) {
      return 0;
    }
    const auto startRow = nextRowNumber();
    VELOX_CHECK_NE(startRow, kAtEnd);
    const auto& range = ranges_[rangeIdx_];
    const auto firstRow = windowFirstRow(range.firstRow, rowInRange_);
    loadWindow(firstRow);

    const auto offsetInWindow = static_cast<vector_size_t>(startRow - firstRow);
    const auto numRows = static_cast<vector_size_t>(rowsToRead);
    std::vector<VectorPtr> children;
    children.reserve(columns_.size());
    for (auto& window : windows_) {
      children.push_back(window->slice(offsetInWindow, numRows));
    }
    auto input = std::make_shared<RowVector>(pool_, inputRowType_, nullptr, numRows, std::move(children));
    result = RowReader::projectColumns(input, *scanSpec_, mutation);

    rowInRange_ += rowsToRead;
    return static_cast<uint64_t>(rowsToRead);
  }

  void loadWindow(int64_t firstRow) {
    if (windowFirstRow_ == firstRow) {
      return;
    }
    windows_.clear();
    windows_.reserve(columns_.size());
    for (size_t i = 0; i < columns_.size(); ++i) {
      auto it = pins_[i].find(firstRow);
      // Eligibility pinned every window of every column, so a miss here is a
      // bug rather than an eviction.
      VELOX_CHECK(
          it != pins_[i].end(), "Decoded cache window {} of column {} is not pinned", firstRow, columns_[i].name);
      auto* entry = it->second.entry();
      std::string buffer;
      buffer.resize(entry->size());
      DecodedCache::copyOut(entry, buffer.data());
      // Opening parses the header and materializes the dictionary once, so the
      // per-read cost below is one slice rather than a whole-window decode.
      auto window = DecodedWindow::open(std::move(buffer), columns_[i].type, pool_);
      VELOX_CHECK_NOT_NULL(window, "Decoded cache window {} of column {} is corrupt", firstRow, columns_[i].name);
      windows_.push_back(std::move(window));
    }
    windowFirstRow_ = firstRow;
  }

  uint64_t nextAndPopulate(uint64_t size, VectorPtr& result, const Mutation* mutation) {
    const auto startRow = delegate_->nextRowNumber();
    if (startRow == kAtEnd) {
      finishPopulate(/*atEnd=*/true);
      return 0;
    }
    // The selective readers require a pre-created result of the right type and
    // reuse its buffers: SelectiveStructColumnReaderBase::next() hands the
    // caller's vector straight to getValues(), which rejects a null one. This
    // mirrors what FileDataSource does with its own output vector, including
    // holding it across calls so buffers can be reused -- Velox reallocates
    // any child still referenced by a previously returned batch.
    if (populateResult_ == nullptr) {
      populateResult_ = BaseVector::create(inputRowType_, 0, pool_);
    }
    // Null mutation: the delegate must not apply deletes, because the rows we
    // cache have to be reusable by queries with a different deletion vector.
    const auto rowsRead = delegate_->next(size, populateResult_, /*mutation=*/nullptr);
    if (rowsRead == 0) {
      finishPopulate(/*atEnd=*/true);
      return 0;
    }
    // The filterless spec projects exactly columns_, in order, so childAt(i)
    // must line up with columns_[i]. Check rather than trust it: a mismatch
    // would cache one column's values under another's key.
    VELOX_CHECK_EQ(
        populateResult_->asUnchecked<RowVector>()->childrenSize(),
        columns_.size(),
        "Decoded cache populate pass produced {} columns, expected {}",
        populateResult_->asUnchecked<RowVector>()->childrenSize(),
        columns_.size());
    // With no filters in the spec, every projected top-level column comes back
    // as a LazyVector: SelectiveStructColumnReaderBase defers a child whenever
    // projectOut && !hasFilter && generateLazyChildren_, and that last one
    // defaults to true with no option to turn it off. Nothing downstream here
    // tolerates a lazy child -- copying one into the staging vector would not
    // materialize it, and ScanSpec::applyFilter casts to SimpleVector<T>. Since
    // this pass exists precisely to decode every row, load them now.
    //
    // The loaded children go into a separate RowVector rather than back into
    // populateResult_, to leave the reader's own reuse state alone.
    auto* lazyRow = populateResult_->asUnchecked<RowVector>();
    std::vector<VectorPtr> loaded;
    loaded.reserve(columns_.size());
    for (size_t i = 0; i < columns_.size(); ++i) {
      loaded.push_back(BaseVector::loadedVectorShared(lazyRow->childAt(i)));
    }
    auto batch = std::make_shared<RowVector>(
        pool_, inputRowType_, nullptr, static_cast<vector_size_t>(rowsRead), std::move(loaded));

    accumulate(startRow, static_cast<int64_t>(rowsRead), batch);
    result = RowReader::projectColumns(batch, *scanSpec_, mutation);
    return rowsRead;
  }

  /// Copies the rows just read into the window staging buffers, flushing each
  /// window as it fills. Ranges are observed rather than computed: a
  /// discontinuity in row numbers means a new row group, and a window must
  /// never span two of them.
  void accumulate(int64_t startRow, int64_t numRows, const VectorPtr& batch) {
    auto* row = batch->as<RowVector>();
    VELOX_CHECK_NOT_NULL(row);
    if (ranges_.empty() || startRow != lastEndRow_) {
      flushWindow();
      ranges_.push_back(WindowRange{startRow, 0});
    }
    ranges_.back().numRows += numRows;
    lastEndRow_ = startRow + numRows;

    const auto rangeFirstRow = ranges_.back().firstRow;
    int64_t done = 0;
    while (done < numRows) {
      const auto rowInRange = startRow + done - rangeFirstRow;
      const auto firstRow = windowFirstRow(rangeFirstRow, rowInRange);
      if (windowFirstRow_ != firstRow) {
        flushWindow();
        beginWindow(firstRow);
      }
      const auto offsetInWindow = static_cast<vector_size_t>(rowInRange - (firstRow - rangeFirstRow));
      const auto take = static_cast<vector_size_t>(std::min<int64_t>(numRows - done, windowRows_ - offsetInWindow));
      for (size_t i = 0; i < columns_.size(); ++i) {
        copyIntoStaging(i, row->childAt(i), offsetInWindow, static_cast<vector_size_t>(done), take);
      }
      windowNumRows_ = offsetInWindow + take;
      done += take;
      if (windowNumRows_ == windowRows_) {
        flushWindow();
      }
    }
  }

  /// Copies 'count' rows of one column into its staging vector.
  ///
  /// Strings are copied value by value rather than with BaseVector::copy.
  /// copyRanges() takes the cheap route for a same-pool StringView source: it
  /// calls acquireSharedStringBuffers() and memcpys the StringView structs, so
  /// the staging vector's strings point into the *reader's* string buffers. A
  /// staging vector outlives many next() calls while Velox recycles the result
  /// vector's buffers underneath it, and the dangling reads that follow crash
  /// in the encoder. FlatVector<StringView>::set() instead copies non-inline
  /// bytes into the staging vector's own buffer, so it owns its data.
  ///
  /// Fixed-width and boolean columns have no such hazard: copyRanges memcpys
  /// values and null bits into buffers the staging vector owns.
  void copyIntoStaging(
      size_t column,
      const VectorPtr& source,
      vector_size_t targetIndex,
      vector_size_t sourceIndex,
      vector_size_t count) {
    const auto kind = columns_[column].type->kind();
    if (kind != TypeKind::VARCHAR && kind != TypeKind::VARBINARY) {
      staging_[column]->copy(source.get(), targetIndex, sourceIndex, count);
      return;
    }
    auto* target = staging_[column]->as<FlatVector<StringView>>();
    VELOX_CHECK_NOT_NULL(target, "Decoded cache staging vector for a string column is not flat");
    // The source may be flat, dictionary or constant -- all of them are
    // SimpleVector<StringView>, so valueAt() resolves any of the three.
    auto* strings = source->as<SimpleVector<StringView>>();
    VELOX_CHECK_NOT_NULL(
        strings, "Decoded cache populate pass produced a non-string vector for {}", columns_[column].name);
    for (vector_size_t i = 0; i < count; ++i) {
      if (strings->isNullAt(sourceIndex + i)) {
        target->setNull(targetIndex + i, true);
      } else {
        target->set(targetIndex + i, strings->valueAt(sourceIndex + i));
      }
    }
  }

  void beginWindow(int64_t firstRow) {
    for (size_t i = 0; i < columns_.size(); ++i) {
      staging_[i] = BaseVector::create(columns_[i].type, static_cast<vector_size_t>(windowRows_), pool_);
    }
    windowFirstRow_ = firstRow;
    windowNumRows_ = 0;
  }

  void flushWindow() {
    if (windowNumRows_ == 0 || windowFirstRow_ < 0) {
      windowFirstRow_ = -1;
      windowNumRows_ = 0;
      return;
    }
    for (size_t i = 0; i < columns_.size(); ++i) {
      auto buffer = encodeWindow(staging_[i], windowNumRows_, pool_);
      if (buffer == nullptr) {
        // A type the window format does not handle. Eligibility should have
        // excluded it, so skip rather than store something unreadable.
        continue;
      }
      const cache::RawFileCacheKey key{columns_[i].keyId, static_cast<uint64_t>(windowFirstRow_)};
      if (cache_->store(key, buffer->as<char>(), buffer->size()) && !columns_[i].hasFilter) {
        // Projection-only data drains to the SSD tier first.
        cache_->makeEvictable(key);
      }
    }
    windowFirstRow_ = -1;
    windowNumRows_ = 0;
  }

  void finishPopulate(bool atEnd) {
    if (populateFinished_) {
      return;
    }
    populateFinished_ = true;
    flushWindow();
    // Only a pass that reached end of split saw every row group it owns. A
    // truncated index (an early-terminated query, a LIMIT) would make later
    // readers drop rows, so it is never written.
    if (!atEnd || ranges_.empty()) {
      return;
    }
    const auto index = encodeSplitIndex(ranges_);
    cache_->store(cache::RawFileCacheKey{splitKeyId_, 0}, index.data(), index.size());
    cache_->stats().splitsTranscoded.fetch_add(1);
  }

  const Mode mode_;
  DecodedCache* const cache_;
  const std::shared_ptr<facebook::velox::common::ScanSpec> scanSpec_;
  memory::MemoryPool* const pool_;
  const std::vector<CachedColumn> columns_;
  RowTypePtr inputRowType_;

  // Serving state.
  std::vector<WindowRange> ranges_;
  std::vector<folly::F14FastMap<int64_t, cache::CachePin>> pins_;
  size_t rangeIdx_{0};
  int64_t rowInRange_{0};
  std::vector<std::unique_ptr<DecodedWindow>> windows_;

  // Populating state.
  std::unique_ptr<RowReader> delegate_;
  uint64_t splitKeyId_{0};
  std::vector<VectorPtr> staging_;
  /// Result vector handed to the delegate, created once and reused.
  VectorPtr populateResult_;
  int64_t lastEndRow_{-1};
  bool populateFinished_{false};

  // Shared by both: the window currently materialized (serving) or being
  // accumulated (populating).
  int64_t windowFirstRow_{-1};
  vector_size_t windowNumRows_{0};
  const int64_t windowRows_;
};

/// Reader that decides, per row reader, between serving from the decoded
/// cache, reading unfiltered and transcoding, or getting out of the way.
class DecodedCacheReader : public Reader {
 public:
  DecodedCacheReader(
      std::unique_ptr<Reader> delegate,
      DecodedCache* cache,
      std::string filePath,
      int64_t fileSize,
      const dwio::common::ReaderOptions& options)
      : delegate_(std::move(delegate)),
        cache_(cache),
        filePath_(std::move(filePath)),
        identity_(cache->fileIdentity(filePath_, fileSize)),
        readerOptions_(options),
        pool_(&options.memoryPool()) {}

  std::optional<uint64_t> numberOfRows() const override {
    return delegate_->numberOfRows();
  }

  std::unique_ptr<ColumnStatistics> columnStatistics(uint32_t index) const override {
    return delegate_->columnStatistics(index);
  }

  const RowTypePtr& rowType() const override {
    return delegate_->rowType();
  }

  const std::shared_ptr<const TypeWithId>& typeWithId() const override {
    return delegate_->typeWithId();
  }

  std::unique_ptr<IndexReader> createIndexReader(const RowReaderOptions& options) const override {
    return delegate_->createIndexReader(options);
  }

  std::unique_ptr<RowReader> createRowReader(const RowReaderOptions& options) const override {
    auto columns = cacheableColumns(options);
    if (!columns.has_value()) {
      cache_->stats().ineligibleSplits.fetch_add(1);
      return delegate_->createRowReader(options);
    }
    const auto contextHash = decodeContextHash(readerOptions_, options);
    const auto splitKeyId = cache_->splitKeyId(filePath_, identity_, options.offset(), options.length(), contextHash);
    if (!splitKeyId.has_value()) {
      return delegate_->createRowReader(options);
    }
    for (auto& column : *columns) {
      auto keyId = cache_->columnKeyId(filePath_, identity_, column.name, contextHash);
      if (!keyId.has_value()) {
        return delegate_->createRowReader(options);
      }
      column.keyId = *keyId;
    }

    if (auto serving = tryCreateServing(options, *columns, *splitKeyId)) {
      cache_->stats().splitsServedFromCache.fetch_add(1);
      return serving;
    }
    if (!cache_->admit(*splitKeyId, 0)) {
      return delegate_->createRowReader(options);
    }
    return createPopulating(options, *columns, *splitKeyId);
  }

 private:
  /// Returns the leaf columns to cache, or nullopt when this read cannot use
  /// the cache at all. Everything rejected here falls back to the untouched
  /// Velox read path.
  std::optional<std::vector<CachedColumn>> cacheableColumns(const RowReaderOptions& options) const {
    const auto& scanSpec = options.scanSpec();
    if (scanSpec == nullptr) {
      return std::nullopt;
    }
    // Synthesized row numbers/ids are produced by the reader, not read from
    // the file, so they would not be in the materialized input.
    if (options.rowNumberColumnInfo().has_value()) {
      return std::nullopt;
    }
    // Row sampling and row skipping both change which rows a read produces.
    // randomSkip is a ReaderOptions setting, skipRows a RowReaderOptions one.
    if (readerOptions_.randomSkip() != nullptr || options.skipRows() != 0) {
      return std::nullopt;
    }
    // A ColumnSelector owns requestedType, and RowReaderOptions refuses to let
    // it be replaced while one is set. The populating pass needs to narrow
    // requestedType to the cached columns, so this path is left alone.
    if (options.selector() != nullptr) {
      return std::nullopt;
    }
    if (!cache_->options().serveFilteredReads && scanSpec->hasFilter()) {
      return std::nullopt;
    }
    const auto& fileType = delegate_->rowType();
    std::vector<CachedColumn> columns;
    for (const auto& childSpec : scanSpec->children()) {
      if (childSpec->isConstant()) {
        // Partition and missing columns are rebuilt by projectColumns.
        continue;
      }
      if (childSpec->deltaUpdate() != nullptr) {
        // projectColumns rejects these outright.
        return std::nullopt;
      }
      // The column must exist in the file...
      if (!fileType->getChildIdxIfExists(childSpec->fieldName()).has_value()) {
        return std::nullopt;
      }
      // ...but the type to cache is the type the query asked for, not the
      // file's. Under schema evolution those differ (INTEGER on disk read as
      // BIGINT), and caching the file type would hand the caller a vector of
      // the wrong type. requestedType is part of the decode context hash, so a
      // different requested type is a different key rather than a wrong hit.
      TypePtr type;
      if (const auto& requestedType = options.requestedType()) {
        if (const auto idx = requestedType->getChildIdxIfExists(childSpec->fieldName())) {
          type = requestedType->childAt(*idx);
        }
      }
      if (type == nullptr) {
        type = fileType->childAt(*fileType->getChildIdxIfExists(childSpec->fieldName()));
      }
      if (!isCacheableType(type)) {
        return std::nullopt;
      }
      columns.push_back(CachedColumn{childSpec->fieldName(), type, 0, childSpec->filter() != nullptr});
    }
    if (columns.empty()) {
      return std::nullopt;
    }
    return columns;
  }

  std::unique_ptr<RowReader> tryCreateServing(
      const RowReaderOptions& options,
      const std::vector<CachedColumn>& columns,
      uint64_t splitKeyId) const {
    auto indexPin = cache_->pin(cache::RawFileCacheKey{splitKeyId, 0});
    if (!indexPin.has_value()) {
      return nullptr;
    }
    std::string buffer;
    buffer.resize(indexPin->entry()->size());
    DecodedCache::copyOut(indexPin->entry(), buffer.data());
    std::vector<WindowRange> ranges;
    if (!decodeSplitIndex(buffer.data(), buffer.size(), ranges) || ranges.empty()) {
      return nullptr;
    }

    const int64_t windowRows = cache_->options().windowRows;
    int64_t pinnedBytes = 0;
    std::vector<folly::F14FastMap<int64_t, cache::CachePin>> pins(columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
      for (const auto& range : ranges) {
        for (int64_t offset = 0; offset < range.numRows; offset += windowRows) {
          const auto firstRow = range.firstRow + offset;
          auto pin = cache_->pin(cache::RawFileCacheKey{columns[i].keyId, static_cast<uint64_t>(firstRow)});
          if (!pin.has_value()) {
            return nullptr;
          }
          pinnedBytes += pin->entry()->size();
          if (pinnedBytes > cache_->options().maxPinnedBytesPerSplit) {
            cache_->stats().rejectedPinBudget.fetch_add(1);
            return nullptr;
          }
          pins[i].emplace(firstRow, std::move(*pin));
        }
      }
    }
    return std::make_unique<DecodedCacheRowReader>(cache_, options, pool_, columns, std::move(ranges), std::move(pins));
  }

  std::unique_ptr<RowReader> createPopulating(
      const RowReaderOptions& options,
      const std::vector<CachedColumn>& columns,
      uint64_t splitKeyId) const {
    // A filterless spec over exactly the columns being cached. Filters and
    // deletes are applied afterwards by projectColumns, so what lands in the
    // cache is reusable by a query with any predicate.
    auto spec = std::make_shared<facebook::velox::common::ScanSpec>("root");
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    names.reserve(columns.size());
    types.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
      auto* child = spec->getOrCreateChild(columns[i].name);
      child->setProjectOut(true);
      child->setChannel(static_cast<column_index_t>(i));
      names.push_back(columns[i].name);
      types.push_back(columns[i].type);
    }
    RowReaderOptions filterless = options;
    filterless.setScanSpec(spec);
    // Keep the requested type consistent with the spec: the selective readers
    // build their column tree from both, and a spec narrower than the
    // requested type is not a shape Velox is asked to handle elsewhere.
    filterless.setRequestedType(ROW(std::move(names), std::move(types)));
    // A metadata filter would exclude row groups by statistics, which would
    // make the observed row ranges depend on this query's predicates.
    filterless.setMetadataFilter(nullptr);
    auto delegateRowReader = delegate_->createRowReader(filterless);
    if (delegateRowReader == nullptr) {
      return delegate_->createRowReader(options);
    }
    return std::make_unique<DecodedCacheRowReader>(
        cache_, options, pool_, columns, std::move(delegateRowReader), splitKeyId);
  }

  const std::unique_ptr<Reader> delegate_;
  DecodedCache* const cache_;
  const std::string filePath_;
  const FileIdentity identity_;
  const dwio::common::ReaderOptions readerOptions_;
  memory::MemoryPool* const pool_;
};

} // namespace

DecodedCacheReaderFactory::DecodedCacheReaderFactory(std::shared_ptr<ReaderFactory> delegate)
    : ReaderFactory(delegate->fileFormat()), delegate_(std::move(delegate)) {}

std::unique_ptr<Reader> DecodedCacheReaderFactory::createReader(
    std::unique_ptr<BufferedInput> input,
    const dwio::common::ReaderOptions& options) {
  // Read the name before the input is moved into the delegate.
  const std::string filePath = input->getName();
  // Taken from the input rather than options.fileHandle(): FileSplitReader
  // only calls setFileHandle() when the raw byte cache is on, and the decoded
  // cache must work without it.
  int64_t fileSize = 0;
  if (const auto& readFile = input->getReadFile()) {
    fileSize = static_cast<int64_t>(readFile->size());
  }
  auto delegate = delegate_->createReader(std::move(input), options);
  auto* cache = DecodedCache::getInstance();
  if (delegate == nullptr || cache == nullptr) {
    return delegate;
  }
  return std::make_unique<DecodedCacheReader>(std::move(delegate), cache, filePath, fileSize, options);
}

std::shared_ptr<FormatSpecificOptions> DecodedCacheReaderFactory::createFormatOptions(
    const config::ConfigBase& connectorConfig,
    const config::ConfigBase& session) const {
  return delegate_->createFormatOptions(connectorConfig, session);
}

void registerDecodedCacheReaderFactory(FileFormat format) {
  if (!hasReaderFactory(format)) {
    LOG(WARNING) << "Decoded cache: no reader factory registered for format " << static_cast<int>(format);
    return;
  }
  auto delegate = getReaderFactory(format);
  unregisterReaderFactory(format);
  registerReaderFactory(std::make_shared<DecodedCacheReaderFactory>(std::move(delegate)));
}

uint64_t decodeContextHash(const dwio::common::ReaderOptions& readerOptions, const RowReaderOptions& rowReaderOptions) {
  // See GLUTEN_DECODED_CACHE_CONTEXT_FIELDS in the header before changing this.
  uint64_t hash = folly::hash::hash_combine(
      static_cast<int32_t>(readerOptions.fileFormat()),
      static_cast<int32_t>(readerOptions.columnMappingMode()),
      readerOptions.adjustTimestampToTimezone(),
      readerOptions.fileColumnNamesReadAsLowerCase(),
      static_cast<int32_t>(rowReaderOptions.timestampPrecision()));
  if (const auto* timezone = readerOptions.sessionTimezone()) {
    hash = folly::hash::hash_combine(hash, timezone->name());
  }
  if (const auto& fileSchema = readerOptions.fileSchema()) {
    hash = folly::hash::hash_combine(hash, fileSchema->toString());
  }
  if (const auto& requestedType = rowReaderOptions.requestedType()) {
    hash = folly::hash::hash_combine(hash, requestedType->toString());
  }
  const auto& formatOptions = rowReaderOptions.formatSpecificOptions();
  if (const auto* parquetOptions = dynamic_cast<const parquet::ParquetReaderOptions*>(formatOptions.get())) {
    hash = folly::hash::hash_combine(
        hash, parquetOptions->allowInt32Narrowing(), parquetOptions->nullStructIfAllFieldsMissing());
  } else if (formatOptions != nullptr) {
    // An unrecognized options object may carry value-affecting settings that
    // are not in the hash. Fail closed by making the key unusable rather than
    // risk serving values decoded under different settings.
    hash = folly::hash::hash_combine(hash, std::string("unknown-format-options"), typeid(*formatOptions).name());
  }
  return hash;
}

} // namespace gluten
