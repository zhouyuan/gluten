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

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <optional>
#include <string>
#include <vector>

#include "operators/functions/RegistrationAllFunctions.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox::functions::sparksql::test;
using namespace facebook::velox;

namespace {
constexpr const char* kSparkAnsiCast = "spark_ansi_cast";
constexpr const char* kSparkLegacyCast = "spark_legacy_cast";

std::string sparkAnsiEnabledConfigKey() {
  return functions::sparksql::SparkQueryConfig::qualify(functions::sparksql::SparkQueryConfig::kAnsiEnabled);
}
} // namespace

class SparkFunctionTest : public SparkFunctionBaseTest {
 public:
  SparkFunctionTest() {
    gluten::registerAllFunctions();
  }

 protected:
  template <typename T>
  void runRoundTest(const std::vector<std::tuple<T, T>>& data) {
    auto result = evaluate<SimpleVector<T>>("round(c0)", makeRowVector({makeFlatVector<T, 0>(data)}));
    for (int32_t i = 0; i < data.size(); ++i) {
      ASSERT_EQ(result->valueAt(i), std::get<1>(data[i]));
    }
  }

  template <typename T>
  void runRoundWithDecimalTest(const std::vector<std::tuple<T, int32_t, T>>& data) {
    auto result = evaluate<SimpleVector<T>>(
        "round(c0, c1)", makeRowVector({makeFlatVector<T, 0>(data), makeFlatVector<int32_t, 1>(data)}));
    for (int32_t i = 0; i < data.size(); ++i) {
      ASSERT_EQ(result->valueAt(i), std::get<2>(data[i]));
    }
  }

  template <typename T>
  std::vector<std::tuple<T, T>> testRoundFloatData() {
    return {
        {1.0, 1.0},
        {1.9, 2.0},
        {1.3, 1.0},
        {0.0, 0.0},
        {0.9999, 1.0},
        {-0.9999, -1.0},
        {1.0 / 9999999, 0},
        {123123123.0 / 9999999, 12.0}};
  }

  template <typename T>
  std::vector<std::tuple<T, T>> testRoundIntegralData() {
    return {{1, 1}, {0, 0}, {-1, -1}};
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testRoundWithDecFloatAndDoubleData() {
    return {{1.122112, 0, 1},       {1.129, 1, 1.1},        {1.129, 2, 1.13},         {1.0 / 3, 0, 0.0},
            {1.0 / 3, 1, 0.3},      {1.0 / 3, 2, 0.33},     {1.0 / 3, 6, 0.333333},   {-1.122112, 0, -1},
            {-1.129, 1, -1.1},      {-1.129, 2, -1.13},     {-1.129, 2, -1.13},       {-1.0 / 3, 0, 0.0},
            {-1.0 / 3, 1, -0.3},    {-1.0 / 3, 2, -0.33},   {-1.0 / 3, 6, -0.333333}, {1.0, -1, 0.0},
            {0.0, -2, 0.0},         {-1.0, -3, 0.0},        {11111.0, -1, 11110.0},   {11111.0, -2, 11100.0},
            {11111.0, -3, 11000.0}, {11111.0, -4, 10000.0}, {0.575, 2, 0.58},         {0.574, 2, 0.57},
            {-0.575, 2, -0.58},     {-0.574, 2, -0.57}};
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testRoundWithDecIntegralData() {
    return {
        {1, 0, 1},
        {0, 0, 0},
        {-1, 0, -1},
        {1, 1, 1},
        {0, 1, 0},
        {-1, 1, -1},
        {1, 10, 1},
        {0, 10, 0},
        {-1, 10, -1},
        {1, -1, 0},
        {0, -2, 0},
        {-1, -3, 0}};
  }
};

// The function overlay re-registers the regexp functions so that patterns
// follow java.util.regex, which is what Spark specifies, rather than raw RE2.
// See cpp/velox/operators/functions/overlay/JavaRegexTranslator.h.
class SparkRegexFunctionTest : public SparkFunctionTest {
 protected:
  std::optional<bool> rlike(std::optional<std::string> str, const std::string& pattern) {
    return evaluateOnce<bool>(fmt::format("rlike(c0, '{}')", pattern), str);
  }

  std::optional<std::string>
  regexpExtract(std::optional<std::string> str, const std::string& pattern, int32_t group) {
    return evaluateOnce<std::string>(fmt::format("regexp_extract(c0, '{}', {})", pattern, group), str);
  }

  std::optional<std::string>
  regexpReplace(std::optional<std::string> str, const std::string& pattern, const std::string& replacement) {
    return evaluateOnce<std::string>(fmt::format("regexp_replace(c0, '{}', '{}')", pattern, replacement), str);
  }

  std::vector<std::string> split(const std::string& str, const std::string& delimiter) {
    auto result = evaluate(
        fmt::format("split(c0, '{}')", delimiter), makeRowVector({makeFlatVector<std::string>({str})}));
    auto* array = result->as<ArrayVector>();
    auto* elements = array->elements()->as<SimpleVector<StringView>>();
    std::vector<std::string> parts;
    for (auto i = 0; i < array->sizeAt(0); ++i) {
      parts.emplace_back(elements->valueAt(array->offsetAt(0) + i).str());
    }
    return parts;
  }
};

TEST_F(SparkRegexFunctionTest, whitespaceClasses) {
  // java.util.regex counts the vertical tab as whitespace, RE2 does not.
  EXPECT_EQ(rlike("\x0b", "^\\s$"), true);
  EXPECT_EQ(rlike("\x0b", "^\\S$"), false);
  EXPECT_EQ(rlike("a\x0b\x62", "^a\\sb$"), true);
  EXPECT_EQ(rlike(" ", "^\\s$"), true);
  EXPECT_EQ(rlike("a", "^\\s$"), false);
  EXPECT_EQ(rlike("a", "^\\S$"), true);
  // A shorthand class nested in a character class keeps working.
  EXPECT_EQ(rlike("\x0b", "^[\\sx]$"), true);
  EXPECT_EQ(rlike("x", "^[\\sx]$"), true);
  EXPECT_EQ(rlike("y", "^[\\sx]$"), false);

  // java.util.regex reads \v as the vertical whitespace class, RE2 as the
  // vertical tab character.
  EXPECT_EQ(rlike("\n", "^\\v$"), true);
  EXPECT_EQ(rlike("\x0b", "^\\v$"), true);
  // U+2028, LINE SEPARATOR.
  EXPECT_EQ(rlike("\xE2\x80\xA8", "^\\v$"), true);
  EXPECT_EQ(rlike("a", "^\\v$"), false);
  EXPECT_EQ(rlike("a", "^\\V$"), true);

  // \h and \H are unknown to RE2.
  EXPECT_EQ(rlike("\t", "^\\h$"), true);
  EXPECT_EQ(rlike(" ", "^\\h$"), true);
  // U+2003, EM SPACE.
  EXPECT_EQ(rlike("\xE2\x80\x83", "^\\h$"), true);
  EXPECT_EQ(rlike("a", "^\\h$"), false);
  EXPECT_EQ(rlike("a", "^\\H$"), true);
}

TEST_F(SparkRegexFunctionTest, lineBreak) {
  // \R is atomic in java.util.regex, so it consumes "\r\n" as a whole.
  EXPECT_EQ(rlike("\r\n", "^\\R$"), true);
  EXPECT_EQ(rlike("\n", "^\\R$"), true);
  EXPECT_EQ(rlike("\r", "^\\R$"), true);
  EXPECT_EQ(rlike("a", "^\\R$"), false);
}

TEST_F(SparkRegexFunctionTest, characterEscapes) {
  // \e, the escape character.
  EXPECT_EQ(rlike("\x1b", "^\\e$"), true);
  // \cX, a control character.
  EXPECT_EQ(rlike("\x01", "^\\cA$"), true);
  EXPECT_EQ(rlike("a", "^\\cA$"), false);
  // \uHHHH, a UTF-16 escape.
  EXPECT_EQ(rlike("A", "^\\u0041$"), true);
  EXPECT_EQ(rlike("B", "^\\u0041$"), false);
  // A surrogate pair is a single code point, U+1F600.
  EXPECT_EQ(rlike("\xF0\x9F\x98\x80", "^\\uD83D\\uDE00$"), true);
  // \Q ... \E quotes a literal and its contents are not translated.
  EXPECT_EQ(rlike("\\s", "^\\Q\\s\\E$"), true);
  EXPECT_EQ(rlike(" ", "^\\Q\\s\\E$"), false);
}

TEST_F(SparkRegexFunctionTest, posixClasses) {
  EXPECT_EQ(rlike("a", "^\\p{Alpha}$"), true);
  EXPECT_EQ(rlike("1", "^\\p{Alpha}$"), false);
  EXPECT_EQ(rlike("1", "^\\P{Alpha}$"), true);
  EXPECT_EQ(rlike("f", "^\\p{XDigit}$"), true);
  EXPECT_EQ(rlike("g", "^\\p{XDigit}$"), false);
  // java.util.regex's \p{Space} matches \s, vertical tab included.
  EXPECT_EQ(rlike("\x0b", "^\\p{Space}$"), true);
  EXPECT_EQ(rlike("~", "^\\p{ASCII}$"), true);
  // U+00E9, LATIN SMALL LETTER E WITH ACUTE.
  EXPECT_EQ(rlike("\xC3\xA9", "^\\p{ASCII}$"), false);
  // Nested in a character class.
  EXPECT_EQ(rlike("5", "^[\\p{Digit}x]$"), true);
  EXPECT_EQ(rlike("x", "^[\\p{Digit}x]$"), true);
  EXPECT_EQ(rlike("y", "^[\\p{Digit}x]$"), false);
  // Unicode categories are spelled the same in both engines.
  EXPECT_EQ(rlike("a", "^\\p{L}$"), true);
}

TEST_F(SparkRegexFunctionTest, namedCapturingGroup) {
  // java.util.regex spells a named group (?<name>...), RE2 (?P<name>...).
  EXPECT_EQ(regexpExtract("abc", "a(?<mid>b)c", 1), "b");
  EXPECT_EQ(regexpReplace("abc", "a(?<mid>b)c", "[$1]"), "[b]");
}

TEST_F(SparkRegexFunctionTest, regexpReplaceAndExtractTranslatePattern) {
  EXPECT_EQ(regexpReplace("a\x0b\x62", "\\s", "-"), "a-b");
  EXPECT_EQ(regexpReplace("a\tb", "\\h", "-"), "a-b");
  EXPECT_EQ(regexpExtract("a\x0b\x62", "a(\\s)b", 1), "\x0b");
}

TEST_F(SparkRegexFunctionTest, splitTranslatesDelimiter) {
  using ::testing::ElementsAre;
  // The vertical tab is whitespace to java.util.regex, so it splits.
  EXPECT_THAT(split("a\x0b\x62", "\\s"), ElementsAre("a", "b"));
  EXPECT_THAT(split("a b\tc", "\\s+"), ElementsAre("a", "b", "c"));
  EXPECT_THAT(split("a\tb", "\\h"), ElementsAre("a", "b"));
  // Delimiters with nothing Java-specific keep their existing behaviour,
  // including the literal fast paths.
  EXPECT_THAT(split("a,b,c", ","), ElementsAre("a", "b", "c"));
  EXPECT_THAT(split("a.b", "\\."), ElementsAre("a", "b"));
  EXPECT_THAT(split("abc", ""), ElementsAre("a", "b", "c"));
}

TEST_F(SparkRegexFunctionTest, unsupportedCharacterClassSetOperations) {
  // RE2 reads the nested '[' and the '&&' as literals, so these have to be
  // rejected rather than quietly matching a different set. The error makes
  // plan validation fall the expression back to vanilla Spark.
  VELOX_ASSERT_THROW(rlike("a", "[a[b]]"), "character class union");
  VELOX_ASSERT_THROW(rlike("a", "[a&&[b]]"), "character class intersection");
  VELOX_ASSERT_THROW(rlike("a", "[a&&[^b]]"), "character class intersection");
  // RE2 cannot nest a complement inside a character class.
  VELOX_ASSERT_THROW(rlike("a", "[\\Sx]"), "inside a character class");
  VELOX_ASSERT_THROW(rlike("a", "[\\Hx]"), "inside a character class");
  VELOX_ASSERT_THROW(rlike("a", "[\\Vx]"), "inside a character class");
}

TEST_F(SparkRegexFunctionTest, constructsNeedingBacktrackingStillFail) {
  // These need backtracking, which RE2 does not do. They must keep failing so
  // that the expression falls back to vanilla Spark instead of being offloaded
  // with the wrong semantics.
  EXPECT_THROW(rlike("a", "(?=a)"), VeloxUserError);
  EXPECT_THROW(rlike("a", "(?!a)"), VeloxUserError);
  EXPECT_THROW(rlike("a", "(?<=a)"), VeloxUserError);
  EXPECT_THROW(rlike("a", "(?<!a)"), VeloxUserError);
  EXPECT_THROW(rlike("aa", "(a)\\1"), VeloxUserError);
  EXPECT_THROW(rlike("a", "a?+"), VeloxUserError);
  EXPECT_THROW(rlike("a", "(?>a)"), VeloxUserError);
  EXPECT_THROW(rlike("a", "\\G"), VeloxUserError);
  EXPECT_THROW(rlike("a", "\\Z"), VeloxUserError);
}

TEST_F(SparkRegexFunctionTest, patternsWithoutJavaSpecificsAreUntouched) {
  EXPECT_EQ(rlike("a", "^[ab]*$"), true);
  EXPECT_EQ(rlike("b", "a+"), false);
  EXPECT_EQ(rlike("a1", "^\\w\\d$"), true);
  EXPECT_EQ(rlike("a-b", "^a\\-b$"), true);
  EXPECT_EQ(regexpExtract("2026-09-11", "(\\d{4})-(\\d{2})", 2), "09");
  EXPECT_EQ(regexpReplace("abc", "b", "X"), "aXc");
}

TEST_F(SparkFunctionTest, round) {
  runRoundTest<float>(testRoundFloatData<float>());
  runRoundTest<double>(testRoundFloatData<double>());
  runRoundTest<int64_t>(testRoundIntegralData<int64_t>());
  runRoundTest<int32_t>(testRoundIntegralData<int32_t>());
  runRoundTest<int16_t>(testRoundIntegralData<int16_t>());
  runRoundTest<int8_t>(testRoundIntegralData<int8_t>());
}

TEST_F(SparkFunctionTest, roundWithDecimal) {
  runRoundWithDecimalTest<float>(testRoundWithDecFloatAndDoubleData<float>());
  runRoundWithDecimalTest<double>(testRoundWithDecFloatAndDoubleData<double>());
  runRoundWithDecimalTest<int64_t>(testRoundWithDecIntegralData<int64_t>());
  runRoundWithDecimalTest<int32_t>(testRoundWithDecIntegralData<int32_t>());
  runRoundWithDecimalTest<int16_t>(testRoundWithDecIntegralData<int16_t>());
  runRoundWithDecimalTest<int8_t>(testRoundWithDecIntegralData<int8_t>());
}

TEST_F(SparkFunctionTest, expressionLevelAnsiCastIgnoresSessionAnsiOff) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "false"}});
  auto input = makeRowVector({makeFlatVector<std::string>({"2147483648"})});
  core::TypedExprPtr field = std::make_shared<const core::FieldAccessTypedExpr>(VARCHAR(), "c0");
  auto ansiCast =
      std::make_shared<const core::CallTypedExpr>(INTEGER(), std::vector<core::TypedExprPtr>{field}, kSparkAnsiCast);

  VELOX_ASSERT_THROW(evaluate(ansiCast, input), "Cannot cast");
}

TEST_F(SparkFunctionTest, expressionLevelLegacyCastIgnoresSessionAnsiOn) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "true"}});
  auto input = makeRowVector({makeFlatVector<int32_t>({1234567})});
  core::TypedExprPtr field = std::make_shared<const core::FieldAccessTypedExpr>(INTEGER(), "c0");
  auto legacyCast =
      std::make_shared<const core::CallTypedExpr>(TINYINT(), std::vector<core::TypedExprPtr>{field}, kSparkLegacyCast);

  facebook::velox::test::assertEqualVectors(makeFlatVector<int8_t>({-121}), evaluate(legacyCast, input));
}
