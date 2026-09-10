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

#include "cache/DecodedWindowFormat.h"

#include "compute/VeloxBackend.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace gluten {

class DecodedWindowFormatTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    VeloxBackend::create(AllocationListener::noop(), {});
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  /// Encodes 'column', reopens the blob, and returns the whole window. The
  /// round trip through a std::string is what the cache does: the blob is
  /// copied out of a cache entry before it is opened.
  VectorPtr roundTrip(const VectorPtr& column, vector_size_t numRows = -1) {
    const auto rows = numRows < 0 ? column->size() : numRows;
    auto blob = encodeWindow(column, rows, pool_.get());
    EXPECT_NE(blob, nullptr);
    auto window = open(blob, column->type());
    EXPECT_NE(window, nullptr);
    EXPECT_EQ(window->numRows(), rows);
    return window->slice(0, rows);
  }

  std::unique_ptr<DecodedWindow> open(const BufferPtr& blob, const TypePtr& type) {
    return DecodedWindow::open(std::string(blob->as<char>(), blob->size()), type, pool_.get());
  }

  WindowEncoding encodingOf(const BufferPtr& blob) {
    return static_cast<WindowEncoding>(reinterpret_cast<const WindowHeader*>(blob->as<char>())->encoding);
  }

  std::shared_ptr<memory::MemoryPool> pool_ = defaultLeafVeloxMemoryPool();
};

TEST_F(DecodedWindowFormatTest, headerLayoutIsFixed) {
  // The blob is only self-describing if the header never silently changes
  // shape. A failure here means every cached window is misread.
  EXPECT_EQ(sizeof(WindowHeader), 48);
  EXPECT_EQ(kWindowMagic, 0x57444c47u);
}

TEST_F(DecodedWindowFormatTest, fixedWidthRoundTrip) {
  auto column = makeNullableFlatVector<int64_t>({1, -2, std::nullopt, 4, 5});
  test::assertEqualVectors(column, roundTrip(column));
}

TEST_F(DecodedWindowFormatTest, allSupportedFixedWidthKinds) {
  test::assertEqualVectors(
      makeNullableFlatVector<int8_t>({1, std::nullopt, -3}),
      roundTrip(makeNullableFlatVector<int8_t>({1, std::nullopt, -3})));
  test::assertEqualVectors(
      makeNullableFlatVector<int16_t>({300, std::nullopt}),
      roundTrip(makeNullableFlatVector<int16_t>({300, std::nullopt})));
  test::assertEqualVectors(makeFlatVector<int32_t>({7, 8}), roundTrip(makeFlatVector<int32_t>({7, 8})));
  test::assertEqualVectors(
      makeNullableFlatVector<float>({1.5f, std::nullopt}),
      roundTrip(makeNullableFlatVector<float>({1.5f, std::nullopt})));
  test::assertEqualVectors(
      makeNullableFlatVector<double>({-0.25, std::nullopt, 1e100}),
      roundTrip(makeNullableFlatVector<double>({-0.25, std::nullopt, 1e100})));
  auto timestamps = makeNullableFlatVector<Timestamp>({Timestamp(1000, 123), std::nullopt, Timestamp(-5, 999999999)});
  test::assertEqualVectors(timestamps, roundTrip(timestamps));
}

TEST_F(DecodedWindowFormatTest, booleanRoundTrip) {
  auto column = makeNullableFlatVector<bool>({true, false, std::nullopt, true, true, false});
  test::assertEqualVectors(column, roundTrip(column));
}

TEST_F(DecodedWindowFormatTest, noNullsIsEncodedWithoutABitmap) {
  auto column = makeFlatVector<int64_t>({1, 2, 3});
  auto blob = encodeWindow(column, column->size(), pool_.get());
  const auto* header = reinterpret_cast<const WindowHeader*>(blob->as<char>());
  EXPECT_EQ(header->flags & kFlagHasNulls, 0);
  test::assertEqualVectors(column, roundTrip(column));
}

TEST_F(DecodedWindowFormatTest, lowCardinalityIntegersUseADictionary) {
  // 400 rows over 3 distinct values: 3 * 8 dictionary bytes + 400 index bytes
  // beats 400 * 8 plain bytes, so the encoder must pick the dictionary.
  std::vector<int64_t> values;
  for (int i = 0; i < 400; ++i) {
    values.push_back(i % 3);
  }
  auto column = makeFlatVector<int64_t>(values);
  auto blob = encodeWindow(column, column->size(), pool_.get());
  EXPECT_EQ(encodingOf(blob), WindowEncoding::kDictionary);
  // 3 distinct values fit in one byte per index.
  EXPECT_EQ(reinterpret_cast<const WindowHeader*>(blob->as<char>())->indexBits, 8);
  EXPECT_LT(blob->size(), column->size() * sizeof(int64_t));
  test::assertEqualVectors(column, roundTrip(column));
}

TEST_F(DecodedWindowFormatTest, highCardinalityIntegersStayPlain) {
  std::vector<int64_t> values;
  for (int i = 0; i < 400; ++i) {
    values.push_back(i * 7777);
  }
  auto column = makeFlatVector<int64_t>(values);
  auto blob = encodeWindow(column, column->size(), pool_.get());
  // A dictionary of 400 distinct 8-byte values plus 400 two-byte indices is
  // larger than storing the values, so plain must win.
  EXPECT_EQ(encodingOf(blob), WindowEncoding::kPlain);
  test::assertEqualVectors(column, roundTrip(column));
}

TEST_F(DecodedWindowFormatTest, dictionaryWithNulls) {
  auto column = makeNullableFlatVector<int32_t>({5, std::nullopt, 5, 6, std::nullopt, 6, 5});
  test::assertEqualVectors(column, roundTrip(column));
}

TEST_F(DecodedWindowFormatTest, repeatedStringsUseADictionary) {
  std::vector<std::string> owned;
  for (int i = 0; i < 200; ++i) {
    owned.push_back(i % 2 == 0 ? "a repeated value long enough to matter" : "another repeated value");
  }
  std::vector<StringView> views;
  for (const auto& s : owned) {
    views.push_back(StringView(s));
  }
  auto column = makeFlatVector<StringView>(views);
  auto blob = encodeWindow(column, column->size(), pool_.get());
  EXPECT_EQ(encodingOf(blob), WindowEncoding::kDictionary);
  test::assertEqualVectors(column, roundTrip(column));
}

TEST_F(DecodedWindowFormatTest, inlineStringsUseADictionary) {
  // Values of twelve bytes or fewer are stored inline in the StringView, so
  // StringView::data() points into the StringView object rather than into a
  // string buffer. A dictionary keyed on a view of a temporary copy therefore
  // dangles the moment the copy dies. This is the TPC-DS date_dim shape --
  // d_day_name, d_quarter_name -- and it segfaulted in production while
  // repeatedStringsUseADictionary passed, because that test's values are 38
  // bytes and so live in a buffer that outlives the loop.
  const std::vector<std::string> days{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
  std::vector<StringView> views;
  for (int i = 0; i < 500; ++i) {
    views.push_back(StringView(days[i % days.size()]));
  }
  for (const auto& view : views) {
    ASSERT_TRUE(view.isInline()) << "test no longer covers the inline case";
  }
  auto column = makeFlatVector<StringView>(views);
  auto blob = encodeWindow(column, column->size(), pool_.get());
  EXPECT_EQ(encodingOf(blob), WindowEncoding::kDictionary);
  EXPECT_EQ(reinterpret_cast<const WindowHeader*>(blob->as<char>())->dictCount, days.size());
  test::assertEqualVectors(column, roundTrip(column));
}

TEST_F(DecodedWindowFormatTest, dictionaryMixesInlineAndBufferedStrings) {
  std::vector<std::string> owned;
  for (int i = 0; i < 300; ++i) {
    owned.push_back(i % 3 == 0 ? "tiny" : "a value well past the inline limit of twelve bytes");
  }
  std::vector<StringView> views;
  for (const auto& s : owned) {
    views.push_back(StringView(s));
  }
  auto column = makeFlatVector<StringView>(views);
  auto blob = encodeWindow(column, column->size(), pool_.get());
  EXPECT_EQ(encodingOf(blob), WindowEncoding::kDictionary);
  test::assertEqualVectors(column, roundTrip(column));
}

TEST_F(DecodedWindowFormatTest, distinctStringsStayPlain) {
  std::vector<std::string> owned;
  for (int i = 0; i < 50; ++i) {
    owned.push_back("distinct-" + std::to_string(i));
  }
  std::vector<StringView> views;
  for (const auto& s : owned) {
    views.push_back(StringView(s));
  }
  auto column = makeFlatVector<StringView>(views);
  auto blob = encodeWindow(column, column->size(), pool_.get());
  EXPECT_EQ(encodingOf(blob), WindowEncoding::kPlain);
  test::assertEqualVectors(column, roundTrip(column));
}

TEST_F(DecodedWindowFormatTest, stringsWithNullsAndEmpties) {
  auto column = makeNullableFlatVector<StringView>(
      {StringView(""),
       std::nullopt,
       StringView("short"),
       StringView("a value comfortably past the twelve byte inline limit"),
       std::nullopt});
  test::assertEqualVectors(column, roundTrip(column));
}

TEST_F(DecodedWindowFormatTest, sliceReturnsOnlyTheRequestedRows) {
  // Partial decode is the point of the format: serving 4096 rows out of a
  // 65536-row window must not materialize the window.
  std::vector<int64_t> values;
  for (int i = 0; i < 1000; ++i) {
    values.push_back(i);
  }
  auto column = makeFlatVector<int64_t>(values);
  auto blob = encodeWindow(column, column->size(), pool_.get());
  auto window = open(blob, column->type());
  ASSERT_NE(window, nullptr);

  auto middle = window->slice(400, 100);
  ASSERT_EQ(middle->size(), 100);
  auto* flat = middle->asFlatVector<int64_t>();
  ASSERT_NE(flat, nullptr);
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(flat->valueAt(i), 400 + i);
  }
  // The window is reusable across slices.
  EXPECT_EQ(window->slice(0, 1)->asFlatVector<int64_t>()->valueAt(0), 0);
  EXPECT_EQ(window->slice(999, 1)->asFlatVector<int64_t>()->valueAt(0), 999);
}

TEST_F(DecodedWindowFormatTest, sliceCarriesNullsAtAnOffset) {
  // Null bits have to be shifted, not just copied, when the slice does not
  // start on a byte boundary.
  std::vector<std::optional<int32_t>> values;
  for (int i = 0; i < 100; ++i) {
    values.push_back(i % 3 == 0 ? std::nullopt : std::optional<int32_t>(i));
  }
  auto column = makeNullableFlatVector<int32_t>(values);
  auto blob = encodeWindow(column, column->size(), pool_.get());
  auto window = open(blob, column->type());
  ASSERT_NE(window, nullptr);
  for (vector_size_t offset : {1, 7, 8, 13, 63, 64}) {
    auto sliced = window->slice(offset, 20);
    ASSERT_EQ(sliced->size(), 20);
    for (vector_size_t i = 0; i < 20; ++i) {
      const auto row = offset + i;
      EXPECT_EQ(sliced->isNullAt(i), !values[row].has_value()) << "offset " << offset << " row " << i;
    }
  }
}

TEST_F(DecodedWindowFormatTest, sliceOfADictionaryWindowAtAnOffset) {
  std::vector<std::optional<int64_t>> values;
  for (int i = 0; i < 300; ++i) {
    values.push_back(i % 5 == 0 ? std::nullopt : std::optional<int64_t>(i % 4));
  }
  auto column = makeNullableFlatVector<int64_t>(values);
  auto blob = encodeWindow(column, column->size(), pool_.get());
  ASSERT_EQ(encodingOf(blob), WindowEncoding::kDictionary);
  auto window = open(blob, column->type());
  ASSERT_NE(window, nullptr);
  auto sliced = window->slice(37, 50);
  ASSERT_EQ(sliced->size(), 50);
  for (vector_size_t i = 0; i < 50; ++i) {
    const auto row = 37 + i;
    if (!values[row].has_value()) {
      EXPECT_TRUE(sliced->isNullAt(i)) << "row " << i;
    } else {
      ASSERT_FALSE(sliced->isNullAt(i)) << "row " << i;
      EXPECT_EQ(sliced->asUnchecked<SimpleVector<int64_t>>()->valueAt(i), *values[row]);
    }
  }
}

TEST_F(DecodedWindowFormatTest, trimsToTheRowsActuallyRead) {
  // A staging vector is allocated for a whole window but may be only partly
  // filled at a row-group boundary; only the rows read may be encoded.
  auto column = makeFlatVector<int32_t>({10, 11, 12, 13, 14, 15});
  auto decoded = roundTrip(column, 4);
  ASSERT_EQ(decoded->size(), 4);
  test::assertEqualVectors(makeFlatVector<int32_t>({10, 11, 12, 13}), decoded);
}

TEST_F(DecodedWindowFormatTest, minMaxRecordedForIntegralKinds) {
  auto column = makeNullableFlatVector<int64_t>({5, std::nullopt, -3, 9});
  auto blob = encodeWindow(column, column->size(), pool_.get());
  const auto* header = reinterpret_cast<const WindowHeader*>(blob->as<char>());
  EXPECT_NE(header->flags & kFlagHasMinMax, 0);
  EXPECT_EQ(header->minValue, -3);
  EXPECT_EQ(header->maxValue, 9);

  // Not claimed for kinds where an int64 range would be meaningless.
  auto doubles = makeFlatVector<double>({1.0, 2.0});
  auto doubleBlob = encodeWindow(doubles, doubles->size(), pool_.get());
  EXPECT_EQ(reinterpret_cast<const WindowHeader*>(doubleBlob->as<char>())->flags & kFlagHasMinMax, 0);
}

TEST_F(DecodedWindowFormatTest, unsupportedTypeIsRefused) {
  auto column = makeArrayVector<int64_t>({{1, 2}, {3}});
  EXPECT_EQ(encodeWindow(column, column->size(), pool_.get()), nullptr);
}

TEST_F(DecodedWindowFormatTest, openRejectsMalformedBlobs) {
  auto column = makeFlatVector<int64_t>({1, 2, 3});
  auto blob = encodeWindow(column, column->size(), pool_.get());
  const std::string good(blob->as<char>(), blob->size());

  // Too short to hold a header.
  EXPECT_EQ(DecodedWindow::open(good.substr(0, 8), BIGINT(), pool_.get()), nullptr);

  // Wrong magic: not our blob.
  auto badMagic = good;
  badMagic[0] = 'X';
  EXPECT_EQ(DecodedWindow::open(badMagic, BIGINT(), pool_.get()), nullptr);

  // Wrong format version: written by a different build.
  auto badVersion = good;
  reinterpret_cast<WindowHeader*>(badVersion.data())->formatVersion = kWindowFormatVersion + 1;
  EXPECT_EQ(DecodedWindow::open(badVersion, BIGINT(), pool_.get()), nullptr);

  // Type mismatch: a key collision that slipped past the decode-context hash
  // must be caught here rather than reinterpreting the bytes.
  EXPECT_EQ(DecodedWindow::open(good, INTEGER(), pool_.get()), nullptr);

  // A well-formed blob still opens.
  EXPECT_NE(DecodedWindow::open(good, BIGINT(), pool_.get()), nullptr);
}

} // namespace gluten
