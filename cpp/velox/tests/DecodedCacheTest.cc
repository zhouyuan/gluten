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

#include "compute/VeloxBackend.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace gluten {

class DecodedCacheTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    VeloxBackend::create(AllocationListener::noop(), {});
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    memory::MmapAllocator::Options options;
    options.capacity = 256 << 20;
    allocator_ = std::make_shared<memory::MmapAllocator>(options);
    asyncDataCache_ = cache::AsyncDataCache::create(allocator_.get());
    DecodedCacheOptions cacheOptions;
    cacheOptions.windowRows = 1024;
    cacheOptions.admitMinTouches = 2;
    decodedCache_ = std::make_shared<DecodedCache>(asyncDataCache_.get(), cacheOptions);
  }

  void TearDown() override {
    decodedCache_.reset();
    asyncDataCache_->shutdown();
    asyncDataCache_.reset();
    allocator_.reset();
  }

  std::shared_ptr<memory::MemoryPool> pool_ = defaultLeafVeloxMemoryPool();
  std::shared_ptr<memory::MmapAllocator> allocator_;
  std::shared_ptr<cache::AsyncDataCache> asyncDataCache_;
  std::shared_ptr<DecodedCache> decodedCache_;
};

TEST_F(DecodedCacheTest, blobRoundTrip) {
  const auto keyId = decodedCache_->columnKeyId("/tmp/a.parquet", FileIdentity{100, 7}, "c0", 0xabcd);
  ASSERT_TRUE(keyId.has_value());
  const cache::RawFileCacheKey key{*keyId, 4096};

  // Larger than one memory page so the non-contiguous page-run path is
  // exercised, which is what dataRanges() exists for.
  std::string payload(300 * 1024, '\0');
  for (size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<char>(i % 251);
  }

  ASSERT_FALSE(decodedCache_->pin(key).has_value());
  ASSERT_TRUE(decodedCache_->store(key, payload.data(), payload.size()));

  auto pin = decodedCache_->pin(key);
  ASSERT_TRUE(pin.has_value());
  ASSERT_EQ(pin->entry()->size(), static_cast<int32_t>(payload.size()));
  std::string readBack;
  readBack.resize(pin->entry()->size());
  DecodedCache::copyOut(pin->entry(), readBack.data());
  EXPECT_EQ(readBack, payload);
}

TEST_F(DecodedCacheTest, entriesAboveSsdLimitAreRefused) {
  const auto keyId = decodedCache_->columnKeyId("/tmp/a.parquet", FileIdentity{100, 7}, "c0", 0);
  ASSERT_TRUE(keyId.has_value());
  const std::string payload(DecodedCache::kMaxEntrySize + 1, 'x');
  EXPECT_FALSE(decodedCache_->store(cache::RawFileCacheKey{*keyId, 0}, payload.data(), payload.size()));
  EXPECT_EQ(decodedCache_->stats().rejectedEntryTooLarge.load(), 1);
}

TEST_F(DecodedCacheTest, keysSeparateWhatMustNotBeShared) {
  const FileIdentity identity{100, 7};
  const auto base = decodedCache_->columnKeyId("/tmp/a.parquet", identity, "c0", 1);
  ASSERT_TRUE(base.has_value());

  // A different file, a rewrite of the same file, a different column, and a
  // different decode context must all land in different namespaces.
  EXPECT_NE(*base, *decodedCache_->columnKeyId("/tmp/b.parquet", identity, "c0", 1));
  EXPECT_NE(*base, *decodedCache_->columnKeyId("/tmp/a.parquet", FileIdentity{101, 7}, "c0", 1));
  EXPECT_NE(*base, *decodedCache_->columnKeyId("/tmp/a.parquet", FileIdentity{100, 8}, "c0", 1));
  EXPECT_NE(*base, *decodedCache_->columnKeyId("/tmp/a.parquet", identity, "c1", 1));
  EXPECT_NE(*base, *decodedCache_->columnKeyId("/tmp/a.parquet", identity, "c0", 2));
  // Same inputs must be stable, otherwise nothing would ever hit.
  EXPECT_EQ(*base, *decodedCache_->columnKeyId("/tmp/a.parquet", identity, "c0", 1));
  // Column and split namespaces must not collide.
  EXPECT_NE(*base, *decodedCache_->splitKeyId("/tmp/a.parquet", identity, 0, 1024, 1));
}

TEST_F(DecodedCacheTest, ghostListDefersAdmission) {
  const auto keyId = decodedCache_->splitKeyId("/tmp/a.parquet", FileIdentity{1, 1}, 0, 128, 0);
  ASSERT_TRUE(keyId.has_value());
  EXPECT_FALSE(decodedCache_->admit(*keyId, 0));
  EXPECT_TRUE(decodedCache_->admit(*keyId, 0));
  EXPECT_TRUE(decodedCache_->admit(*keyId, 0));
  // A different window of the same split is tracked independently.
  EXPECT_FALSE(decodedCache_->admit(*keyId, 1024));
}

TEST_F(DecodedCacheTest, splitIndexRoundTrip) {
  const std::vector<WindowRange> ranges{{0, 1000}, {1000, 500}, {4096, 10}};
  const auto encoded = encodeSplitIndex(ranges);
  std::vector<WindowRange> decoded;
  ASSERT_TRUE(decodeSplitIndex(encoded.data(), encoded.size(), decoded));
  ASSERT_EQ(decoded.size(), ranges.size());
  for (size_t i = 0; i < ranges.size(); ++i) {
    EXPECT_EQ(decoded[i].firstRow, ranges[i].firstRow);
    EXPECT_EQ(decoded[i].numRows, ranges[i].numRows);
  }
}

TEST_F(DecodedCacheTest, splitIndexRejectsGarbage) {
  std::vector<WindowRange> decoded;
  // Too short to hold a header.
  EXPECT_FALSE(decodeSplitIndex("abc", 3, decoded));
  // Right header, truncated payload: a partially written or stale blob must be
  // rejected rather than read as fewer ranges, which would drop rows.
  auto encoded = encodeSplitIndex({{0, 1000}, {1000, 500}});
  EXPECT_FALSE(decodeSplitIndex(encoded.data(), encoded.size() - 1, decoded));
  // Wrong magic.
  encoded[0] = 'X';
  EXPECT_FALSE(decodeSplitIndex(encoded.data(), encoded.size(), decoded));
}

TEST_F(DecodedCacheTest, cacheableTypes) {
  EXPECT_TRUE(isCacheableType(BIGINT()));
  EXPECT_TRUE(isCacheableType(VARCHAR()));
  EXPECT_TRUE(isCacheableType(TIMESTAMP()));
  EXPECT_FALSE(isCacheableType(ARRAY(BIGINT())));
  EXPECT_FALSE(isCacheableType(MAP(VARCHAR(), BIGINT())));
  EXPECT_FALSE(isCacheableType(ROW({"a"}, {BIGINT()})));
  EXPECT_FALSE(isCacheableType(nullptr));
}

TEST_F(DecodedCacheTest, fileIdentityFallsBackToSizeOnly) {
  decodedCache_->registerFile("/tmp/known.parquet", 4096, 12345);
  const auto known = decodedCache_->fileIdentity("/tmp/known.parquet", 0);
  EXPECT_EQ(known.fileSize, 4096);
  EXPECT_EQ(known.modificationTime, 12345);

  // A file never registered keeps the size the reader can see and marks the
  // modification time unknown.
  const auto unknown = decodedCache_->fileIdentity("/tmp/unknown.parquet", 512);
  EXPECT_EQ(unknown.fileSize, 512);
  EXPECT_EQ(unknown.modificationTime, -1);
}

} // namespace gluten
