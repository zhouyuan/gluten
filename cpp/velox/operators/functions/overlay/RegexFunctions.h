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

namespace gluten {

/// Re-registers Velox's 'rlike', 'regexp_extract', 'regexp_extract_all',
/// 'regexp_replace' and 'split' with the pattern run through
/// translateJavaRegexToRe2() first, so that they follow java.util.regex rather
/// than raw RE2 syntax. See JavaRegexTranslator.h for the constructs this
/// covers.
///
/// 'like' is left alone: its pattern is SQL LIKE, not java.util.regex.
///
/// One path stays untranslated: 'regexp_extract_all' compiles a non-constant
/// pattern per row inside Velox, from the argument vector rather than from the
/// factory, so the overlay cannot reach it. Constant patterns, which is what
/// Spark plans almost always carry, are covered.
void registerRegexpFunctions();

} // namespace gluten
