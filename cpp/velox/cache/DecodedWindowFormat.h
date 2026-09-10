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

#include <memory>
#include <string>

#include "velox/buffer/Buffer.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

/// On-cache format for one decoded column window ("Layer 2" in
/// docs/developers/VeloxDecodedCache.md). This is the only part of the decoded
/// cache's persistent representation that Gluten defines; the surrounding
/// container (regions, entry index, checkpoints) belongs to Velox's SsdFile.
///
/// It replaces the phase-1 representation, which was a PrestoVectorSerde blob.
/// That was correct but had three problems this format fixes:
///
///   1. Unvalidatable. A borrowed wire format carries no magic or version of
///      ours, so a stale or foreign blob would be misread rather than
///      rejected. Every window here starts with a WindowHeader.
///   2. All-or-nothing decode. Serving 4096 rows out of a 65536-row window
///      deserialized the whole window. Here a window is opened once and then
///      sliced.
///   3. Uncompressed. Roughly decoded-Arrow sized. Here low-cardinality
///      columns pay one narrow index per row instead of a full value.
///
/// Layout, all sections 8-byte aligned, offsets relative to the blob start:
///
///   WindowHeader                                            48 bytes
///   [nulls bitmap]         if kFlagHasNulls                 ceil(numRows/8)
///   values / indices       at header.valuesOffset
///   [dictionary]           if kDictionary, at header.dictOffset
///
/// Nulls follow Velox's convention: a *set* bit means not null
/// (bits::kNotNull).
namespace gluten {

enum class WindowEncoding : uint8_t {
  /// Values stored one per row: a fixed-width array, a bitmap for BOOLEAN, or
  /// offsets + chars for strings.
  kPlain = 0,
  /// Distinct values stored once, plus one narrow index per row.
  kDictionary = 1,
};

/// Fixed-size prologue of every window blob.
struct WindowHeader {
  /// 'GLDW'. First thing checked on open; a mismatch means the blob is not
  /// ours.
  uint32_t magic;
  /// Bumped on any layout change. Also folded into the cache key namespace via
  /// DecodedCache::kFormatVersion, so a mismatch normally cannot be reached --
  /// this is the backstop for when it is.
  uint16_t formatVersion;
  /// WindowEncoding.
  uint8_t encoding;
  /// kFlagHasNulls | kFlagHasMinMax.
  uint8_t flags;
  /// velox::TypeKind of the encoded column, validated against the type the
  /// reader asks for. Catches a key collision that slipped past the
  /// decode-context hash.
  uint8_t typeKind;
  /// Width of one index in bits: 0 when plain, else 8, 16 or 32.
  ///
  /// Byte-aligned widths only, for now. True bit-packing (any width from 1 to
  /// 32) is a strictly better encoding and needs no format change -- only a
  /// packer, and a reader that feeds Velox's BitPackDecoder. Byte alignment
  /// already captures most of the win for typical low-cardinality columns
  /// while keeping the first cut of the encoder obviously correct.
  uint8_t indexBits;
  uint16_t reserved;
  uint32_t numRows;
  /// Number of distinct values; 0 when plain.
  uint32_t dictCount;
  uint32_t nullsOffset;
  /// Packed indices when kDictionary, the value array when kPlain.
  uint32_t valuesOffset;
  /// Dictionary payload; 0 when kPlain.
  uint32_t dictOffset;
  /// Value range of the non-null rows, set for integral kinds only
  /// (kFlagHasMinMax). Written but not yet read: using it to skip a window
  /// whose whole range fails a filter requires evaluating the filter before
  /// materializing, which arrives with the readWithVisitor path. Recording it
  /// now keeps that from being a format change later.
  int64_t minValue;
  int64_t maxValue;
};

static_assert(sizeof(WindowHeader) == 48, "WindowHeader layout must stay fixed");

constexpr uint32_t kWindowMagic = 0x57444c47; // 'GLDW' little-endian
constexpr uint16_t kWindowFormatVersion = 1;
constexpr uint8_t kFlagHasNulls = 1 << 0;
constexpr uint8_t kFlagHasMinMax = 1 << 1;

/// Encodes the first 'numRows' rows of 'column'. Picks kDictionary or kPlain by
/// measuring both and taking the smaller, so a high-cardinality column is not
/// penalised by a dictionary that cannot pay for itself.
///
/// Returns nullptr when the type is not one this format handles, which leaves
/// the caller to skip caching rather than fall back to a second format.
facebook::velox::BufferPtr encodeWindow(
    const facebook::velox::VectorPtr& column,
    facebook::velox::vector_size_t numRows,
    facebook::velox::memory::MemoryPool* pool);

/// A window blob opened for reading. Holds the parsed header and, for
/// kDictionary, the dictionary materialized once; slice() then costs one index
/// widening per read rather than a full decode.
class DecodedWindow {
 public:
  /// Validates and takes ownership of 'blob'. Returns nullptr if the blob is
  /// not a well-formed window of 'type' -- wrong magic, version, type kind, or
  /// internally inconsistent offsets.
  static std::unique_ptr<DecodedWindow>
  open(std::string blob, const facebook::velox::TypePtr& type, facebook::velox::memory::MemoryPool* pool);

  facebook::velox::vector_size_t numRows() const {
    return numRows_;
  }

  /// Materializes rows [offset, offset + count) as a vector of the type passed
  /// to open(). For kDictionary the result wraps the shared dictionary, so
  /// repeated slices of one window do not copy values.
  facebook::velox::VectorPtr slice(facebook::velox::vector_size_t offset, facebook::velox::vector_size_t count);

 private:
  DecodedWindow() = default;

  facebook::velox::BufferPtr sliceNulls(facebook::velox::vector_size_t offset, facebook::velox::vector_size_t count)
      const;

  std::string blob_;
  facebook::velox::TypePtr type_;
  facebook::velox::memory::MemoryPool* pool_{nullptr};
  const WindowHeader* header_{nullptr};
  facebook::velox::vector_size_t numRows_{0};
  const uint64_t* nulls_{nullptr};
  const char* values_{nullptr};
  /// Dictionary values, built on open() and shared by every slice().
  facebook::velox::VectorPtr dictionary_;
  /// Backing store for plain string chars, kept alive for the StringViews that
  /// point into it.
  facebook::velox::BufferPtr chars_;
  /// Offsets of plain strings, indexed by row.
  const uint32_t* stringOffsets_{nullptr};
};

} // namespace gluten
