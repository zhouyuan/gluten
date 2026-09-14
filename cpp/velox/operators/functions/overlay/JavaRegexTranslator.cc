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

#include "operators/functions/overlay/JavaRegexTranslator.h"

#include <folly/container/F14Map.h>

#include "velox/common/base/Exceptions.h"

namespace gluten {
namespace {

// java.util.regex: \s == [ \t\n\x0B\f\r]. RE2's \s leaves out \x0B.
constexpr std::string_view kSpace = R"(\x{09}\x{0A}\x{0B}\x{0C}\x{0D}\x{20})";

// java.util.regex: \h, see java.util.regex.Pattern.HorizWS. Unknown to RE2.
constexpr std::string_view kHorizontalSpace =
    R"(\x{09}\x{20}\x{A0}\x{1680}\x{180E}\x{2000}-\x{200A}\x{202F}\x{205F}\x{3000})";

// java.util.regex: \v, see java.util.regex.Pattern.VertWS. RE2 reads \v as
// the vertical tab character rather than as a class.
constexpr std::string_view kVerticalSpace = R"(\x{0A}\x{0B}\x{0C}\x{0D}\x{85}\x{2028}\x{2029})";

// java.util.regex: \R. \r\n is listed first so that it is preferred over a
// bare \r, which is how \R behaves in Java, where it is atomic. RE2 resolves
// an alternation leftmost-first, same as Perl.
constexpr std::string_view kLineBreak = R"((?:\x{0D}\x{0A}|[\x{0A}\x{0B}\x{0C}\x{0D}\x{85}\x{2028}\x{2029}]))";

// The all-ASCII range, for java.util.regex's \p{ASCII}. RE2 has no POSIX
// class for it.
constexpr std::string_view kAscii = R"(\x{00}-\x{7F})";

// java.util.regex's POSIX class names, which are US-ASCII only, mapped to the
// RE2 POSIX class of the same contents. RE2 does not know Java's spelling, so
// without this mapping the whole expression falls back to Spark.
const folly::F14FastMap<std::string_view, std::string_view>& posixClasses() {
  static const folly::F14FastMap<std::string_view, std::string_view> kClasses = {
      {"Lower", "lower"},
      {"Upper", "upper"},
      {"Alpha", "alpha"},
      {"Digit", "digit"},
      {"Alnum", "alnum"},
      {"Punct", "punct"},
      {"Graph", "graph"},
      {"Print", "print"},
      {"Blank", "blank"},
      {"Cntrl", "cntrl"},
      {"XDigit", "xdigit"},
      {"Space", "space"},
  };
  return kClasses;
}

class Translator {
 public:
  explicit Translator(std::string_view pattern) : pattern_(pattern) {
    out_.reserve(pattern.size());
  }

  std::string translate() {
    while (pos_ < pattern_.size()) {
      const char c = pattern_[pos_];
      if (c == '\\') {
        translateEscape();
        continue;
      }
      if (inClass_) {
        translateInClass(c);
        continue;
      }
      if (c == '[') {
        inClass_ = true;
      } else if (c == '(' && translateGroupStart()) {
        continue;
      }
      out_ += c;
      ++pos_;
    }
    return std::move(out_);
  }

 private:
  bool startsWith(std::string_view prefix) const {
    return pattern_.substr(pos_).starts_with(prefix);
  }

  // Emits a character class. Inside an enclosing class only the bare contents
  // can be emitted, since RE2 has no way to nest one class in another.
  void emitClass(std::string_view chars, bool negated, std::string_view original) {
    if (inClass_) {
      VELOX_USER_CHECK(
          !negated,
          "Gluten does not support {} inside a character class: RE2 cannot nest a negated class.",
          original);
      out_ += chars;
      return;
    }
    out_ += negated ? "[^" : "[";
    out_ += chars;
    out_ += ']';
  }

  void emitCodePoint(uint32_t codePoint) {
    char buf[16];
    const auto size = std::snprintf(buf, sizeof(buf), "\\x{%02X}", codePoint);
    out_.append(buf, size);
  }

  // java.util.regex reads a nested '[' as class union and '&&' as class
  // intersection. RE2 reads both as literal characters and would quietly
  // match a different set, so reject the pattern and let Spark evaluate it.
  void translateInClass(char c) {
    VELOX_USER_CHECK_NE(
        c,
        '[',
        "Gluten does not support character class union ('[a[b]]'): RE2 reads the nested '[' as a literal.");
    if (c == '&' && pos_ + 1 < pattern_.size() && pattern_[pos_ + 1] == '&') {
      VELOX_USER_FAIL(
          "Gluten does not support character class intersection or difference ('[a&&[b]]', "
          "'[a&&[^b]]'): RE2 reads the '&&' as literal characters.");
    }
    if (c == ']') {
      inClass_ = false;
    }
    out_ += c;
    ++pos_;
  }

  // Rewrites a named capturing group, which java.util.regex spells
  // '(?<name>...)' and RE2 spells '(?P<name>...)'. Returns true when the
  // position was advanced.
  bool translateGroupStart() {
    if (!startsWith("(?<")) {
      return false;
    }
    // '(?<=...)' and '(?<!...)' are lookbehind, not a named group. Leave them
    // alone so that RE2 rejects them and the expression falls back.
    const char next = pos_ + 3 < pattern_.size() ? pattern_[pos_ + 3] : '\0';
    if (next == '=' || next == '!') {
      return false;
    }
    out_ += "(?P<";
    pos_ += 3;
    return true;
  }

  void translateEscape() {
    VELOX_USER_CHECK_LT(pos_ + 1, pattern_.size(), "Pattern ends with a dangling backslash.");
    const char escaped = pattern_[pos_ + 1];
    switch (escaped) {
      case 's':
        emitClass(kSpace, false, "\\s");
        pos_ += 2;
        return;
      case 'S':
        emitClass(kSpace, true, "\\S");
        pos_ += 2;
        return;
      case 'h':
        emitClass(kHorizontalSpace, false, "\\h");
        pos_ += 2;
        return;
      case 'H':
        emitClass(kHorizontalSpace, true, "\\H");
        pos_ += 2;
        return;
      case 'v':
        emitClass(kVerticalSpace, false, "\\v");
        pos_ += 2;
        return;
      case 'V':
        emitClass(kVerticalSpace, true, "\\V");
        pos_ += 2;
        return;
      case 'R':
        VELOX_USER_CHECK(!inClass_, "\\R is not a character class and cannot appear inside one.");
        out_ += kLineBreak;
        pos_ += 2;
        return;
      case 'e':
        emitCodePoint(0x1B);
        pos_ += 2;
        return;
      case 'c':
        translateControlEscape();
        return;
      case 'u':
        translateUnicodeEscape();
        return;
      case 'p':
      case 'P':
        translateNamedClass();
        return;
      case 'Q':
        copyQuoted();
        return;
      default:
        // Everything else means the same thing in both engines, or is a
        // construct RE2 rejects when it compiles the translated pattern.
        out_ += '\\';
        out_ += escaped;
        pos_ += 2;
        return;
    }
  }

  // '\cX' is the control character X ^ 64, see java.util.regex.Pattern.c().
  void translateControlEscape() {
    VELOX_USER_CHECK_LT(pos_ + 2, pattern_.size(), "Illegal control escape sequence.");
    const auto c = static_cast<unsigned char>(pattern_[pos_ + 2]);
    VELOX_USER_CHECK_LT(c, 0x80, "Illegal control escape sequence: '\\c' needs an ASCII character.");
    emitCodePoint(static_cast<uint32_t>(c ^ 0x40));
    pos_ += 3;
  }

  bool readHex4(size_t at, uint32_t& value) const {
    if (at + 4 > pattern_.size()) {
      return false;
    }
    value = 0;
    for (size_t i = at; i < at + 4; ++i) {
      const char c = pattern_[i];
      uint32_t digit;
      if (c >= '0' && c <= '9') {
        digit = c - '0';
      } else if (c >= 'a' && c <= 'f') {
        digit = c - 'a' + 10;
      } else if (c >= 'A' && c <= 'F') {
        digit = c - 'A' + 10;
      } else {
        return false;
      }
      value = (value << 4) | digit;
    }
    return true;
  }

  void translateUnicodeEscape() {
    uint32_t high;
    VELOX_USER_CHECK(readHex4(pos_ + 2, high), "Illegal Unicode escape sequence: '\\u' needs four hex digits.");
    size_t end = pos_ + 6;

    // A surrogate pair, e.g. '😀', is a single code point to RE2.
    if (high >= 0xD800 && high <= 0xDBFF && end + 1 < pattern_.size() && pattern_[end] == '\\' &&
        pattern_[end + 1] == 'u') {
      uint32_t low;
      if (readHex4(end + 2, low) && low >= 0xDC00 && low <= 0xDFFF) {
        emitCodePoint(0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00));
        pos_ = end + 6;
        return;
      }
    }

    emitCodePoint(high);
    pos_ = end;
  }

  void translateNamedClass() {
    const bool negated = pattern_[pos_ + 1] == 'P';
    if (pos_ + 2 >= pattern_.size() || pattern_[pos_ + 2] != '{') {
      // The single letter form, '\pL', means the same in both engines.
      out_ += '\\';
      out_ += pattern_[pos_ + 1];
      pos_ += 2;
      return;
    }
    const auto close = pattern_.find('}', pos_ + 3);
    VELOX_USER_CHECK_NE(close, std::string_view::npos, "Unclosed character class name in pattern.");
    const auto name = pattern_.substr(pos_ + 3, close - (pos_ + 3));

    if (name == "ASCII") {
      emitClass(kAscii, negated, "\\P{ASCII}");
    } else if (const auto it = posixClasses().find(name); it != posixClasses().end()) {
      // RE2 accepts '[:^alpha:]' inside a class, so a negated POSIX class
      // needs no special handling here.
      std::string posix = negated ? "[:^" : "[:";
      posix += it->second;
      posix += ":]";
      if (inClass_) {
        out_ += posix;
      } else {
        out_ += '[';
        out_ += posix;
        out_ += ']';
      }
    } else {
      // A Unicode script or category name. RE2 shares Java's spelling for the
      // ones it supports and rejects the rest.
      out_.append(pattern_.substr(pos_, close + 1 - pos_));
    }
    pos_ = close + 1;
  }

  // '\Q...\E' quotes a literal in both engines, so copy it verbatim rather
  // than translating the escapes it contains.
  void copyQuoted() {
    const auto end = pattern_.find("\\E", pos_ + 2);
    const auto stop = end == std::string_view::npos ? pattern_.size() : end + 2;
    out_.append(pattern_.substr(pos_, stop - pos_));
    pos_ = stop;
  }

  const std::string_view pattern_;
  std::string out_;
  size_t pos_{0};
  bool inClass_{false};
};

} // namespace

std::string translateJavaRegexToRe2(std::string_view pattern) {
  return Translator(pattern).translate();
}

} // namespace gluten
