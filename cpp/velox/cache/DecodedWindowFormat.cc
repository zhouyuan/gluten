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

#include <algorithm>
#include <limits>

#include <folly/container/F14Map.h>

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Nulls.h"
#include "velox/vector/FlatVector.h"

namespace gluten {

using namespace facebook::velox;

namespace {

/// Longest single string the encoder will accept. Well past any real column
/// value, and small enough that a garbage StringView length fails the budget
/// check rather than being memcpy'd.
constexpr uint64_t kMaxStringLength = 1ULL << 30;

constexpr uint64_t align8(uint64_t n) {
  return (n + 7) & ~7ULL;
}

/// Bytes reserved for a bitmap of 'numRows' bits. The 8 bytes of slack cover
/// bits::copyBits, whose tail stores can touch bytes past the last full word.
uint64_t bitmapBytes(vector_size_t numRows) {
  return align8(bits::nbytes(numRows) + 8);
}

/// Bytes of one value of a fixed-width kind. BOOLEAN is excluded: it is stored
/// as a bitmap, not an array.
int32_t physicalWidth(TypeKind kind) {
  switch (kind) {
    case TypeKind::TINYINT:
      return 1;
    case TypeKind::SMALLINT:
      return 2;
    case TypeKind::INTEGER:
    case TypeKind::REAL:
      return 4;
    case TypeKind::BIGINT:
    case TypeKind::DOUBLE:
      return 8;
    case TypeKind::HUGEINT:
      return 16;
    case TypeKind::TIMESTAMP:
      return sizeof(Timestamp);
    default:
      return 0;
  }
}

uint8_t indexBitsFor(uint32_t dictCount) {
  if (dictCount <= 256) {
    return 8;
  }
  if (dictCount <= 65536) {
    return 16;
  }
  return 32;
}

void writeIndex(char* base, uint8_t indexBits, vector_size_t row, uint32_t value) {
  switch (indexBits) {
    case 8:
      reinterpret_cast<uint8_t*>(base)[row] = static_cast<uint8_t>(value);
      return;
    case 16:
      reinterpret_cast<uint16_t*>(base)[row] = static_cast<uint16_t>(value);
      return;
    default:
      reinterpret_cast<uint32_t*>(base)[row] = value;
      return;
  }
}

uint32_t readIndex(const char* base, uint8_t indexBits, vector_size_t row) {
  switch (indexBits) {
    case 8:
      return reinterpret_cast<const uint8_t*>(base)[row];
    case 16:
      return reinterpret_cast<const uint16_t*>(base)[row];
    default:
      return reinterpret_cast<const uint32_t*>(base)[row];
  }
}

/// Fills the header and returns the blob. 'sections' are already-sized, in
/// order: nulls (0 when absent), values, dictionary (0 when absent).
struct Layout {
  uint64_t nullsOffset{0};
  uint64_t valuesOffset{0};
  uint64_t dictOffset{0};
  uint64_t total{0};
};

Layout layout(vector_size_t numRows, bool hasNulls, uint64_t valuesBytes, uint64_t dictBytes) {
  Layout out;
  uint64_t off = sizeof(WindowHeader);
  if (hasNulls) {
    out.nullsOffset = off;
    off += bitmapBytes(numRows);
  }
  out.valuesOffset = off;
  off += align8(valuesBytes);
  if (dictBytes > 0) {
    out.dictOffset = off;
    off += align8(dictBytes);
  }
  out.total = off;
  return out;
}

BufferPtr allocateBlob(const Layout& l, memory::MemoryPool* pool) {
  auto buffer = AlignedBuffer::allocate<char>(l.total, pool);
  ::memset(buffer->asMutable<char>(), 0, l.total);
  return buffer;
}

void fillHeader(
    char* blob,
    const Layout& l,
    WindowEncoding encoding,
    TypeKind kind,
    vector_size_t numRows,
    bool hasNulls,
    uint32_t dictCount,
    uint8_t indexBits) {
  auto* header = reinterpret_cast<WindowHeader*>(blob);
  header->magic = kWindowMagic;
  header->formatVersion = kWindowFormatVersion;
  header->encoding = static_cast<uint8_t>(encoding);
  header->flags = hasNulls ? kFlagHasNulls : 0;
  header->typeKind = static_cast<uint8_t>(kind);
  header->indexBits = indexBits;
  header->reserved = 0;
  header->numRows = static_cast<uint32_t>(numRows);
  header->dictCount = dictCount;
  header->nullsOffset = static_cast<uint32_t>(l.nullsOffset);
  header->valuesOffset = static_cast<uint32_t>(l.valuesOffset);
  header->dictOffset = static_cast<uint32_t>(l.dictOffset);
  header->minValue = 0;
  header->maxValue = 0;
}

void copyNulls(const BaseVector& source, char* blob, const Layout& l, vector_size_t numRows) {
  if (l.nullsOffset == 0) {
    return;
  }
  bits::copyBits(
      source.rawNulls(), 0, reinterpret_cast<uint64_t*>(blob + l.nullsOffset), 0, static_cast<uint64_t>(numRows));
}

template <typename T>
void writeFixedPlain(const FlatVector<T>* flat, char* blob, const Layout& l, vector_size_t numRows) {
  ::memcpy(blob + l.valuesOffset, flat->rawValues(), static_cast<uint64_t>(numRows) * sizeof(T));
}

/// Fixed-width kinds with no dictionary: floating point, HUGEINT, TIMESTAMP.
/// Kept separate from the dictionary encoder so that no hash of T is required.
template <typename T>
BufferPtr encodePlainFixed(const FlatVector<T>* flat, vector_size_t numRows, memory::MemoryPool* pool) {
  const auto kind = flat->typeKind();
  // A bitmap is written whenever the source has one at all: it costs
  // ceil(numRows/8) bytes and avoids having to prove no row is null.
  const bool storeNulls = flat->rawNulls() != nullptr;
  const uint64_t plainBytes = static_cast<uint64_t>(numRows) * sizeof(T);
  const auto l = layout(numRows, storeNulls, plainBytes, 0);
  auto buffer = allocateBlob(l, pool);
  auto* blob = buffer->asMutable<char>();
  fillHeader(blob, l, WindowEncoding::kPlain, kind, numRows, storeNulls, 0, 0);
  copyNulls(*flat, blob, l, numRows);
  writeFixedPlain(flat, blob, l, numRows);
  return buffer;
}

/// Integral kinds, where a dictionary is worth attempting and an int64
/// min/max is meaningful.
template <typename T>
BufferPtr encodeIntegral(const FlatVector<T>* flat, vector_size_t numRows, memory::MemoryPool* pool) {
  const auto kind = flat->typeKind();
  const bool storeNulls = flat->rawNulls() != nullptr;
  const uint64_t plainBytes = static_cast<uint64_t>(numRows) * sizeof(T);

  folly::F14FastMap<T, uint32_t> ids;
  std::vector<T> distinct;
  ids.reserve(std::min<size_t>(numRows, 1024));
  int64_t minValue = std::numeric_limits<int64_t>::max();
  int64_t maxValue = std::numeric_limits<int64_t>::min();
  bool anyValue = false;
  for (vector_size_t i = 0; i < numRows; ++i) {
    if (flat->isNullAt(i)) {
      continue;
    }
    const auto value = flat->valueAt(i);
    if (ids.emplace(value, static_cast<uint32_t>(distinct.size())).second) {
      distinct.push_back(value);
    }
    const auto wide = static_cast<int64_t>(value);
    minValue = std::min(minValue, wide);
    maxValue = std::max(maxValue, wide);
    anyValue = true;
  }

  // Keep the dictionary only if it is actually smaller than storing every
  // value, so a high-cardinality column is not penalised.
  uint8_t indexBits = 0;
  uint64_t dictBytes = 0;
  bool useDictionary = false;
  if (!distinct.empty()) {
    indexBits = indexBitsFor(static_cast<uint32_t>(distinct.size()));
    dictBytes = distinct.size() * sizeof(T);
    const uint64_t indexBytes = static_cast<uint64_t>(numRows) * (indexBits / 8);
    useDictionary = dictBytes + indexBytes < plainBytes;
  }

  const uint64_t valuesBytes = useDictionary ? static_cast<uint64_t>(numRows) * (indexBits / 8) : plainBytes;
  const auto l = layout(numRows, storeNulls, valuesBytes, useDictionary ? dictBytes : 0);
  auto buffer = allocateBlob(l, pool);
  auto* blob = buffer->asMutable<char>();
  fillHeader(
      blob,
      l,
      useDictionary ? WindowEncoding::kDictionary : WindowEncoding::kPlain,
      kind,
      numRows,
      storeNulls,
      useDictionary ? static_cast<uint32_t>(distinct.size()) : 0,
      useDictionary ? indexBits : 0);
  copyNulls(*flat, blob, l, numRows);

  if (useDictionary) {
    ::memcpy(blob + l.dictOffset, distinct.data(), dictBytes);
    char* indices = blob + l.valuesOffset;
    for (vector_size_t i = 0; i < numRows; ++i) {
      // A null row's index is unused but must stay in range, so it points at
      // the first dictionary entry rather than being left arbitrary.
      const uint32_t id = flat->isNullAt(i) ? 0 : ids.find(flat->valueAt(i))->second;
      writeIndex(indices, indexBits, i, id);
    }
  } else {
    writeFixedPlain(flat, blob, l, numRows);
  }

  if (anyValue) {
    auto* header = reinterpret_cast<WindowHeader*>(blob);
    header->flags |= kFlagHasMinMax;
    header->minValue = minValue;
    header->maxValue = maxValue;
  }
  return buffer;
}

BufferPtr encodeBoolean(const FlatVector<bool>* flat, vector_size_t numRows, memory::MemoryPool* pool) {
  const bool storeNulls = flat->rawNulls() != nullptr;
  const auto valuesBytes = bitmapBytes(numRows);
  const auto l = layout(numRows, storeNulls, valuesBytes, 0);
  auto buffer = allocateBlob(l, pool);
  auto* blob = buffer->asMutable<char>();
  fillHeader(blob, l, WindowEncoding::kPlain, TypeKind::BOOLEAN, numRows, storeNulls, 0, 0);
  copyNulls(*flat, blob, l, numRows);
  auto* target = reinterpret_cast<uint64_t*>(blob + l.valuesOffset);
  for (vector_size_t i = 0; i < numRows; ++i) {
    if (!flat->isNullAt(i) && flat->valueAt(i)) {
      bits::setBit(target, i);
    }
  }
  return buffer;
}

BufferPtr encodeString(const FlatVector<StringView>* flat, vector_size_t numRows, memory::MemoryPool* pool) {
  const auto kind = flat->typeKind();
  const bool storeNulls = flat->rawNulls() != nullptr;
  // These hold by construction, but a violation reads a wild StringView and
  // segfaults somewhere inside this function with no usable context. Checking
  // turns that into a reported error naming the numbers involved.
  VELOX_CHECK_LE(numRows, flat->size(), "Decoded cache string window exceeds its staging vector");
  VELOX_CHECK_NOT_NULL(flat->rawValues(), "Decoded cache string window has no values buffer");

  uint64_t totalChars = 0;
  vector_size_t maxLength = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    if (!flat->isNullAt(i)) {
      const auto size = flat->valueAt(i).size();
      totalChars += size;
      maxLength = std::max<vector_size_t>(maxLength, static_cast<vector_size_t>(size));
    }
  }
  // A dangling or uninitialized StringView shows up here as an absurd length
  // long before it is dereferenced.
  VELOX_CHECK_LE(
      totalChars,
      static_cast<uint64_t>(numRows) * kMaxStringLength,
      "Decoded cache string window claims {} chars over {} rows, longest {}: the staged "
      "StringViews are not valid",
      totalChars,
      numRows,
      maxLength);
  const uint64_t plainBytes = (static_cast<uint64_t>(numRows) + 1) * sizeof(uint32_t) + totalChars;

  // Keyed on StringView by value, not on a std::string_view built from
  // valueAt(). StringView::data() returns prefix_ -- a pointer into the object
  // itself -- for values of twelve bytes or fewer, so a view onto a local copy
  // dangles as soon as that copy dies. Velox deletes data() on an rvalue
  // StringView to catch exactly this, which binding the result to a named
  // local sidesteps. Holding StringView by value instead copies the inline
  // bytes into the map node and the vector element, so data() stays valid for
  // as long as the container holds the entry; longer values keep pointing into
  // the vector's own string buffer, which outlives this function.
  folly::F14FastMap<StringView, uint32_t> ids;
  std::vector<StringView> distinct;
  uint64_t dictChars = 0;
  ids.reserve(std::min<size_t>(numRows, 1024));
  for (vector_size_t i = 0; i < numRows; ++i) {
    if (flat->isNullAt(i)) {
      continue;
    }
    const auto value = flat->valueAt(i);
    if (ids.emplace(value, static_cast<uint32_t>(distinct.size())).second) {
      distinct.push_back(value);
      dictChars += value.size();
    }
  }
  uint8_t indexBits = 0;
  uint64_t dictBytes = 0;
  bool useDictionary = false;
  if (!distinct.empty()) {
    indexBits = indexBitsFor(static_cast<uint32_t>(distinct.size()));
    dictBytes = (distinct.size() + 1) * sizeof(uint32_t) + dictChars;
    const uint64_t indexBytes = static_cast<uint64_t>(numRows) * (indexBits / 8);
    useDictionary = dictBytes + indexBytes < plainBytes;
  }

  const uint64_t valuesBytes = useDictionary ? static_cast<uint64_t>(numRows) * (indexBits / 8) : plainBytes;
  const auto l = layout(numRows, storeNulls, valuesBytes, useDictionary ? dictBytes : 0);
  auto buffer = allocateBlob(l, pool);
  auto* blob = buffer->asMutable<char>();
  fillHeader(
      blob,
      l,
      useDictionary ? WindowEncoding::kDictionary : WindowEncoding::kPlain,
      kind,
      numRows,
      storeNulls,
      useDictionary ? static_cast<uint32_t>(distinct.size()) : 0,
      useDictionary ? indexBits : 0);
  copyNulls(*flat, blob, l, numRows);

  if (useDictionary) {
    auto* offsets = reinterpret_cast<uint32_t*>(blob + l.dictOffset);
    char* chars = blob + l.dictOffset + (distinct.size() + 1) * sizeof(uint32_t);
    uint32_t at = 0;
    for (size_t i = 0; i < distinct.size(); ++i) {
      offsets[i] = at;
      VELOX_CHECK_LE(
          at + distinct[i].size(), dictChars, "Decoded cache dictionary overruns its char budget at entry {}", i);
      ::memcpy(chars + at, distinct[i].data(), distinct[i].size());
      at += static_cast<uint32_t>(distinct[i].size());
    }
    offsets[distinct.size()] = at;
    VELOX_CHECK_EQ(at, dictChars, "Decoded cache dictionary wrote a different length than it measured");
    char* indices = blob + l.valuesOffset;
    for (vector_size_t i = 0; i < numRows; ++i) {
      uint32_t id = 0;
      if (!flat->isNullAt(i)) {
        const auto it = ids.find(flat->valueAt(i));
        VELOX_CHECK(it != ids.end(), "Decoded cache dictionary is missing the value at row {}", i);
        id = it->second;
      }
      writeIndex(indices, indexBits, i, id);
    }
  } else {
    auto* offsets = reinterpret_cast<uint32_t*>(blob + l.valuesOffset);
    char* chars = blob + l.valuesOffset + (static_cast<uint64_t>(numRows) + 1) * sizeof(uint32_t);
    uint32_t at = 0;
    for (vector_size_t i = 0; i < numRows; ++i) {
      offsets[i] = at;
      if (!flat->isNullAt(i)) {
        const auto value = flat->valueAt(i);
        VELOX_CHECK_LE(
            at + value.size(), totalChars, "Decoded cache string window overruns its char budget at row {}", i);
        ::memcpy(chars + at, value.data(), value.size());
        at += static_cast<uint32_t>(value.size());
      }
    }
    offsets[numRows] = at;
    VELOX_CHECK_EQ(at, totalChars, "Decoded cache string window wrote a different length than it measured");
  }
  return buffer;
}

/// Normalizes to a flat vector of exactly 'numRows' rows. The populate path
/// already stages flat vectors; this only guards against another encoding
/// arriving.
VectorPtr flatten(const VectorPtr& column, vector_size_t numRows, memory::MemoryPool* pool) {
  if (column->encoding() == VectorEncoding::Simple::FLAT && column->size() >= numRows) {
    return column;
  }
  auto flat = BaseVector::create(column->type(), numRows, pool);
  flat->copy(column.get(), 0, 0, numRows);
  return flat;
}

template <typename T>
VectorPtr makeFlatFromRaw(const TypePtr& type, const char* values, vector_size_t count, memory::MemoryPool* pool) {
  auto vector = BaseVector::create(type, count, pool);
  auto* flat = vector->asUnchecked<FlatVector<T>>();
  ::memcpy(flat->mutableRawValues(), values, static_cast<uint64_t>(count) * sizeof(T));
  return vector;
}

} // namespace

BufferPtr encodeWindow(const VectorPtr& column, vector_size_t numRows, memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(column);
  VELOX_CHECK_GT(numRows, 0);
  const auto kind = column->typeKind();
  if (kind != TypeKind::BOOLEAN && kind != TypeKind::VARCHAR && kind != TypeKind::VARBINARY &&
      physicalWidth(kind) == 0) {
    return nullptr;
  }
  auto flat = flatten(column, numRows, pool);
  switch (kind) {
    case TypeKind::BOOLEAN:
      return encodeBoolean(flat->asUnchecked<FlatVector<bool>>(), numRows, pool);
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return encodeString(flat->asUnchecked<FlatVector<StringView>>(), numRows, pool);
    case TypeKind::TINYINT:
      return encodeIntegral<int8_t>(flat->asUnchecked<FlatVector<int8_t>>(), numRows, pool);
    case TypeKind::SMALLINT:
      return encodeIntegral<int16_t>(flat->asUnchecked<FlatVector<int16_t>>(), numRows, pool);
    case TypeKind::INTEGER:
      return encodeIntegral<int32_t>(flat->asUnchecked<FlatVector<int32_t>>(), numRows, pool);
    case TypeKind::BIGINT:
      return encodeIntegral<int64_t>(flat->asUnchecked<FlatVector<int64_t>>(), numRows, pool);
    case TypeKind::HUGEINT:
      return encodePlainFixed<int128_t>(flat->asUnchecked<FlatVector<int128_t>>(), numRows, pool);
    case TypeKind::REAL:
      return encodePlainFixed<float>(flat->asUnchecked<FlatVector<float>>(), numRows, pool);
    case TypeKind::DOUBLE:
      return encodePlainFixed<double>(flat->asUnchecked<FlatVector<double>>(), numRows, pool);
    case TypeKind::TIMESTAMP:
      return encodePlainFixed<Timestamp>(flat->asUnchecked<FlatVector<Timestamp>>(), numRows, pool);
    default:
      return nullptr;
  }
}

std::unique_ptr<DecodedWindow> DecodedWindow::open(std::string blob, const TypePtr& type, memory::MemoryPool* pool) {
  if (blob.size() < sizeof(WindowHeader)) {
    return nullptr;
  }
  auto window = std::unique_ptr<DecodedWindow>(new DecodedWindow());
  window->blob_ = std::move(blob);
  window->type_ = type;
  window->pool_ = pool;
  const auto* header = reinterpret_cast<const WindowHeader*>(window->blob_.data());
  if (header->magic != kWindowMagic || header->formatVersion != kWindowFormatVersion) {
    return nullptr;
  }
  if (header->typeKind != static_cast<uint8_t>(type->kind())) {
    return nullptr;
  }
  const auto numRows = static_cast<vector_size_t>(header->numRows);
  if (numRows <= 0) {
    return nullptr;
  }
  const auto size = window->blob_.size();
  // Every section must lie inside the blob. A truncated or corrupt entry is
  // rejected rather than read past.
  if (header->valuesOffset >= size || header->nullsOffset >= size || header->dictOffset >= size) {
    return nullptr;
  }
  const auto encoding = static_cast<WindowEncoding>(header->encoding);
  if (encoding == WindowEncoding::kDictionary && (header->dictCount == 0 || header->indexBits == 0)) {
    return nullptr;
  }

  window->header_ = header;
  window->numRows_ = numRows;
  const char* base = window->blob_.data();
  if ((header->flags & kFlagHasNulls) != 0) {
    window->nulls_ = reinterpret_cast<const uint64_t*>(base + header->nullsOffset);
  }
  window->values_ = base + header->valuesOffset;

  const auto kind = type->kind();
  if (encoding == WindowEncoding::kDictionary) {
    const auto dictCount = static_cast<vector_size_t>(header->dictCount);
    const char* dict = base + header->dictOffset;
    if (kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY) {
      const auto* offsets = reinterpret_cast<const uint32_t*>(dict);
      const char* chars = dict + (static_cast<uint64_t>(dictCount) + 1) * sizeof(uint32_t);
      auto charsBytes = offsets[dictCount];
      window->chars_ = AlignedBuffer::allocate<char>(std::max<uint32_t>(charsBytes, 1), pool);
      ::memcpy(window->chars_->asMutable<char>(), chars, charsBytes);
      auto vector = BaseVector::create(type, dictCount, pool);
      auto* flat = vector->asUnchecked<FlatVector<StringView>>();
      auto* raw = flat->mutableRawValues();
      const char* charsBase = window->chars_->as<char>();
      for (vector_size_t i = 0; i < dictCount; ++i) {
        raw[i] = StringView(charsBase + offsets[i], offsets[i + 1] - offsets[i]);
      }
      flat->setStringBuffers({window->chars_});
      window->dictionary_ = vector;
    } else {
      switch (kind) {
        case TypeKind::TINYINT:
          window->dictionary_ = makeFlatFromRaw<int8_t>(type, dict, dictCount, pool);
          break;
        case TypeKind::SMALLINT:
          window->dictionary_ = makeFlatFromRaw<int16_t>(type, dict, dictCount, pool);
          break;
        case TypeKind::INTEGER:
          window->dictionary_ = makeFlatFromRaw<int32_t>(type, dict, dictCount, pool);
          break;
        case TypeKind::BIGINT:
          window->dictionary_ = makeFlatFromRaw<int64_t>(type, dict, dictCount, pool);
          break;
        default:
          // Only the kinds encodeIntegral() handles can be dictionary-encoded.
          return nullptr;
      }
    }
  } else if (kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY) {
    window->stringOffsets_ = reinterpret_cast<const uint32_t*>(window->values_);
    const char* chars = window->values_ + (static_cast<uint64_t>(numRows) + 1) * sizeof(uint32_t);
    const auto charsBytes = window->stringOffsets_[numRows];
    window->chars_ = AlignedBuffer::allocate<char>(std::max<uint32_t>(charsBytes, 1), pool);
    ::memcpy(window->chars_->asMutable<char>(), chars, charsBytes);
  }
  return window;
}

BufferPtr DecodedWindow::sliceNulls(vector_size_t offset, vector_size_t count) const {
  if (nulls_ == nullptr) {
    return nullptr;
  }
  auto buffer = AlignedBuffer::allocate<bool>(count, pool_, bits::kNotNull);
  bits::copyBits(nulls_, static_cast<uint64_t>(offset), buffer->asMutable<uint64_t>(), 0, static_cast<uint64_t>(count));
  return buffer;
}

VectorPtr DecodedWindow::slice(vector_size_t offset, vector_size_t count) {
  VELOX_CHECK_GE(offset, 0);
  VELOX_CHECK_LE(offset + count, numRows_);
  auto nulls = sliceNulls(offset, count);
  const auto kind = type_->kind();

  if (static_cast<WindowEncoding>(header_->encoding) == WindowEncoding::kDictionary) {
    // Widen the packed indices to the int32 a DictionaryVector needs. The
    // dictionary itself is shared, so this is the only per-read copy.
    auto indices = AlignedBuffer::allocate<vector_size_t>(count, pool_);
    auto* raw = indices->asMutable<vector_size_t>();
    for (vector_size_t i = 0; i < count; ++i) {
      raw[i] = static_cast<vector_size_t>(readIndex(values_, header_->indexBits, offset + i));
    }
    return BaseVector::wrapInDictionary(std::move(nulls), std::move(indices), count, dictionary_);
  }

  if (kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY) {
    auto vector = BaseVector::create(type_, count, pool_);
    auto* flat = vector->asUnchecked<FlatVector<StringView>>();
    auto* raw = flat->mutableRawValues();
    const char* charsBase = chars_->as<char>();
    for (vector_size_t i = 0; i < count; ++i) {
      const auto row = offset + i;
      raw[i] = StringView(charsBase + stringOffsets_[row], stringOffsets_[row + 1] - stringOffsets_[row]);
    }
    flat->setStringBuffers({chars_});
    if (nulls != nullptr) {
      vector->setNulls(nulls);
    }
    return vector;
  }

  if (kind == TypeKind::BOOLEAN) {
    auto vector = BaseVector::create(type_, count, pool_);
    auto* flat = vector->asUnchecked<FlatVector<bool>>();
    auto* raw = reinterpret_cast<uint64_t*>(flat->mutableRawValues());
    bits::copyBits(
        reinterpret_cast<const uint64_t*>(values_),
        static_cast<uint64_t>(offset),
        raw,
        0,
        static_cast<uint64_t>(count));
    if (nulls != nullptr) {
      vector->setNulls(nulls);
    }
    return vector;
  }

  const auto width = physicalWidth(kind);
  VELOX_CHECK_GT(width, 0);
  const char* from = values_ + static_cast<uint64_t>(offset) * width;
  VectorPtr vector;
  switch (kind) {
    case TypeKind::TINYINT:
      vector = makeFlatFromRaw<int8_t>(type_, from, count, pool_);
      break;
    case TypeKind::SMALLINT:
      vector = makeFlatFromRaw<int16_t>(type_, from, count, pool_);
      break;
    case TypeKind::INTEGER:
      vector = makeFlatFromRaw<int32_t>(type_, from, count, pool_);
      break;
    case TypeKind::BIGINT:
      vector = makeFlatFromRaw<int64_t>(type_, from, count, pool_);
      break;
    case TypeKind::HUGEINT:
      vector = makeFlatFromRaw<int128_t>(type_, from, count, pool_);
      break;
    case TypeKind::REAL:
      vector = makeFlatFromRaw<float>(type_, from, count, pool_);
      break;
    case TypeKind::DOUBLE:
      vector = makeFlatFromRaw<double>(type_, from, count, pool_);
      break;
    case TypeKind::TIMESTAMP:
      vector = makeFlatFromRaw<Timestamp>(type_, from, count, pool_);
      break;
    default:
      VELOX_FAIL("Decoded cache window has unsupported type {}", type_->toString());
  }
  if (nulls != nullptr) {
    vector->setNulls(nulls);
  }
  return vector;
}

} // namespace gluten
