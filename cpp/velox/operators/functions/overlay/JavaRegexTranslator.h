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

#include <string>
#include <string_view>

namespace gluten {

/// Rewrites a java.util.regex pattern into an RE2 pattern with the same
/// meaning. Spark's regexp functions are specified in terms of
/// java.util.regex, while Velox evaluates them with RE2, and the two disagree
/// on a handful of constructs.
///
/// Translated, because RE2 accepts them but assigns a different meaning:
///   \s \S     java.util.regex includes \x0B in \s, RE2 does not.
///   \v \V     java.util.regex reads \v as the vertical whitespace class,
///             RE2 reads it as the vertical tab character.
///
/// Translated, because RE2 rejects them and the expression would otherwise
/// fall back to Spark unnecessarily:
///   \h \H     Horizontal whitespace class.
///   \R        Any Unicode linebreak.
///   \e        The escape character, \x1B.
///   \cX       Control character X.
///   \uHHHH    UTF-16 escape, including surrogate pairs.
///   \p{Alpha} java.util.regex's POSIX class names, and \P{...} of the same.
///   (?<n>...) Named capturing group; RE2 spells it (?P<n>...).
///
/// Rejected with a user error, because RE2 cannot express them and would
/// otherwise silently match something else:
///   [a[b]] [a&&[b]] [a&&[^b]]
///             Character class union, intersection and difference. RE2 reads
///             the nested '[' and the '&&' as literal characters.
///   [...\S...]
///             A negated shorthand class nested in a character class. RE2 has
///             no way to nest a complement.
///
/// Passed through unchanged, so that RE2 rejects the translated pattern:
///   Lookaround, backreferences, possessive quantifiers, (?>...), \G and \Z.
///   These need backtracking, which RE2 does not do. Failing here is the
///   intended outcome: Gluten's plan validation treats the error as a
///   validation failure and lets vanilla Spark evaluate the expression.
///
/// Returns the pattern unchanged when it uses none of the constructs above.
/// Throws VeloxUserError for the rejected cases.
std::string translateJavaRegexToRe2(std::string_view pattern);

} // namespace gluten
