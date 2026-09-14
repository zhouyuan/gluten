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

#include "operators/functions/overlay/RegexFunctions.h"

#include <optional>
#include <string>
#include <vector>

#include "memory/VeloxMemoryManager.h"
#include "operators/functions/overlay/JavaRegexTranslator.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/Udf.h"
#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/lib/string/StringImpl.h"
#include "velox/functions/sparksql/RegexFunctions.h"
#include "velox/functions/sparksql/Split.h"

using namespace facebook;
using namespace facebook::velox;

namespace gluten {
namespace {

// Returns 'inputArgs' with the constant pattern at 'patternIndex' rewritten
// into RE2 syntax. A non-constant pattern is left as it is; Velox rejects
// those for rlike and regexp_extract regardless.
std::vector<exec::VectorFunctionArg> translatePatternArg(
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    size_t patternIndex) {
  if (inputArgs.size() <= patternIndex || !inputArgs[patternIndex].type->isVarchar()) {
    return inputArgs;
  }
  const auto& constantPattern = inputArgs[patternIndex].constantValue;
  if (constantPattern == nullptr || constantPattern->isNullAt(0)) {
    return inputArgs;
  }

  const auto pattern = constantPattern->as<ConstantVector<StringView>>()->valueAt(0);
  const std::string_view original{pattern.data(), pattern.size()};
  auto translated = translateJavaRegexToRe2(original);
  if (translated == original) {
    return inputArgs;
  }

  auto args = inputArgs;
  args[patternIndex] = exec::VectorFunctionArg{
      inputArgs[patternIndex].type,
      BaseVector::createConstant(VARCHAR(), Variant(translated), 1, defaultLeafVeloxMemoryPool().get())};
  return args;
}

std::shared_ptr<exec::VectorFunction> makeRLike(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  return functions::sparksql::makeRLike(name, translatePatternArg(inputArgs, 1), config);
}

std::shared_ptr<exec::VectorFunction> makeRegexExtract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  return functions::sparksql::makeRegexExtract(name, translatePatternArg(inputArgs, 1), config);
}

std::shared_ptr<exec::VectorFunction> makeRegexExtractAll(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  return functions::makeRe2ExtractAll(name, translatePatternArg(inputArgs, 1), config);
}

std::string preparePattern(const StringView& pattern) {
  const auto translated = translateJavaRegexToRe2(std::string_view{pattern.data(), pattern.size()});
  return functions::prepareRegexpReplacePattern(StringView(translated));
}

// regexp_replace(string, pattern, overwrite[, position]) -> string
//
// A copy of Velox's sparksql regexp_replace, with the pattern additionally
// translated from java.util.regex to RE2 syntax. Velox prepares the pattern
// through a free function that cannot be substituted from the outside, hence
// the copy. Keep in sync with velox/functions/sparksql/RegexFunctions.cpp
// until the translation lands upstream.
template <typename T>
struct RegexpReplaceFunction {
  RegexpReplaceFunction() : cache_(0) {}

  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const arg_type<Varchar>* stringInput,
      const arg_type<Varchar>* pattern,
      const arg_type<Varchar>* replacement) {
    initialize(inputTypes, config, stringInput, pattern, replacement, nullptr);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*stringInput*/,
      const arg_type<Varchar>* pattern,
      const arg_type<Varchar>* replacement,
      const arg_type<int32_t>* /*position*/) {
    if (pattern) {
      const auto processedPattern = preparePattern(*pattern);
      re_.emplace(processedPattern, RE2::Quiet);
      VELOX_USER_CHECK(re_->ok(), "Invalid regular expression {}: {}.", processedPattern, re_->error());

      if (replacement) {
        // Only when both the 'replacement' and 'pattern' are constants can
        // they be processed during initialization; otherwise, each row needs
        // to be processed separately.
        constantReplacement_ = functions::prepareRegexpReplaceReplacement(re_.value(), *replacement);
      }
    }
    cache_.setMaxCompiledRegexes(config.exprMaxCompiledRegexes());
  }

  void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& stringInput,
      const arg_type<Varchar>& pattern,
      const arg_type<Varchar>& replacement) {
    call(result, stringInput, pattern, replacement, 1);
  }

  void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& stringInput,
      const arg_type<Varchar>& pattern,
      const arg_type<Varchar>& replacement,
      const arg_type<int32_t>& position) {
    if (performChecks(result, stringInput, pattern, replacement, position - 1)) {
      return;
    }
    size_t start = functions::stringImpl::cappedByteLength<false>(stringInput, position - 1);
    if (start > stringInput.size() + 1) {
      result = stringInput;
      return;
    }
    performReplace(result, stringInput, pattern, replacement, start);
  }

  void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& stringInput,
      const arg_type<Varchar>& pattern,
      const arg_type<Varchar>& replacement) {
    callAscii(result, stringInput, pattern, replacement, 1);
  }

  void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& stringInput,
      const arg_type<Varchar>& pattern,
      const arg_type<Varchar>& replacement,
      const arg_type<int32_t>& position) {
    if (performChecks(result, stringInput, pattern, replacement, position - 1)) {
      return;
    }
    performReplace(result, stringInput, pattern, replacement, position - 1);
  }

 private:
  bool performChecks(
      out_type<Varchar>& result,
      const arg_type<Varchar>& stringInput,
      const arg_type<Varchar>& pattern,
      const arg_type<Varchar>& replace,
      const arg_type<int32_t>& position) {
    VELOX_USER_CHECK_GE(position + 1, 1, "regexp_replace requires a position >= 1");
    if (position > stringInput.size()) {
      result = stringInput;
      return true;
    }

    if (stringInput.size() == 0 && pattern.size() == 0 && position == 1) {
      result = replace;
      return true;
    }
    return false;
  }

  void performReplace(
      out_type<Varchar>& result,
      const arg_type<Varchar>& stringInput,
      const arg_type<Varchar>& pattern,
      const arg_type<Varchar>& replace,
      const arg_type<int32_t>& position) {
    auto& re = ensurePattern(pattern);
    const auto& processedReplacement = constantReplacement_.has_value()
        ? constantReplacement_.value()
        : functions::prepareRegexpReplaceReplacement(re, replace);

    std::string prefix(stringInput.data(), position);
    std::string targetString(stringInput.data() + position, stringInput.size() - position);

    RE2::GlobalReplace(&targetString, re, processedReplacement);
    result = prefix + targetString;
  }

  RE2& ensurePattern(const arg_type<Varchar>& pattern) {
    if (re_.has_value()) {
      return re_.value();
    }
    auto processedPattern = preparePattern(pattern);
    return *cache_.findOrCompile(StringView(processedPattern));
  }

  // Used when pattern is constant.
  std::optional<RE2> re_;

  // Used when replacement is constant.
  std::optional<std::string> constantReplacement_;

  // Used when pattern is not constant.
  functions::detail::ReCache cache_;
};

// split(string, delimiter[, limit]) -> array(varchar)
//
// Spark's split delimiter is a java.util.regex pattern. Delegates to Velox's
// sparksql split with the delimiter translated to RE2 syntax; the delegate
// reads the delimiter from the arguments it is handed, both for its literal
// fast paths and for the regex path, so translating on the way in is enough.
template <typename T>
struct SplitFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument, same as the delegate.
  static constexpr int32_t reuse_strings_from_arg = 0;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const arg_type<Varchar>* input,
      const arg_type<Varchar>* delimiter) {
    initialize(inputTypes, config, input, delimiter, nullptr);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const arg_type<Varchar>* input,
      const arg_type<Varchar>* delimiter,
      const arg_type<int32_t>* limit) {
    if (delimiter == nullptr) {
      delegate_.initialize(inputTypes, config, input, nullptr, limit);
      return;
    }
    constantDelimiter_ = translateDelimiter(*delimiter);
    constantDelimiterView_ = StringView(constantDelimiter_.value());
    delegate_.initialize(inputTypes, config, input, &constantDelimiterView_, limit);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<Varchar>>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& delimiter) {
    delegate_.call(result, input, ensureDelimiter(delimiter));
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<Varchar>>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& delimiter,
      const arg_type<int32_t>& limit) {
    delegate_.call(result, input, ensureDelimiter(delimiter), limit);
  }

 private:
  static std::string translateDelimiter(const StringView& delimiter) {
    return translateJavaRegexToRe2(std::string_view{delimiter.data(), delimiter.size()});
  }

  const StringView& ensureDelimiter(const arg_type<Varchar>& delimiter) {
    if (constantDelimiter_.has_value()) {
      return constantDelimiterView_;
    }
    rowDelimiter_ = translateDelimiter(delimiter);
    rowDelimiterView_ = StringView(rowDelimiter_);
    return rowDelimiterView_;
  }

  velox::functions::sparksql::Split<T> delegate_;

  // The translated delimiter, and a view of it. Both are kept alive for the
  // whole call because the delegate only holds a view.
  std::optional<std::string> constantDelimiter_;
  StringView constantDelimiterView_;

  // The same, for a delimiter that is not constant and so is translated per
  // row.
  std::string rowDelimiter_;
  StringView rowDelimiterView_;
};

} // namespace

void registerRegexpFunctions() {
  exec::registerStatefulVectorFunction("rlike", functions::re2SearchSignatures(), makeRLike);
  exec::registerStatefulVectorFunction("regexp_extract", functions::re2ExtractSignatures(), makeRegexExtract);
  exec::registerStatefulVectorFunction(
      "regexp_extract_all", functions::re2ExtractAllSignatures(), makeRegexExtractAll);

  registerFunction<RegexpReplaceFunction, Varchar, Varchar, Varchar, Varchar>({"regexp_replace"});
  registerFunction<RegexpReplaceFunction, Varchar, Varchar, Varchar, Varchar, int32_t>({"regexp_replace"});

  registerFunction<SplitFunction, Array<Varchar>, Varchar, Varchar>({"split"});
  registerFunction<SplitFunction, Array<Varchar>, Varchar, Varchar, int32_t>({"split"});
}

} // namespace gluten
