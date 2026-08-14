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

/// Validates the Velox query config keys Gluten writes against the property
/// registry of the Velox it is linked against.
///
/// Gluten populates roughly fifty Velox query config properties. When a Velox
/// update renames, removes or deregisters one of them, a reference to the
/// corresponding Velox constant fails to compile and the breakage is obvious.
/// But three cases are silent today, and all three lose a setting without any
/// diagnostic:
///
///   1. Velox keeps the constant but drops the property from its registry, so
///      nothing reads the value anymore.
///   2. Gluten writes a key as a raw string literal rather than via a Velox
///      constant, and the literal goes stale.
///   3. A user passes a key through the `spark.gluten.sql.columnar.backend.velox.`
///      or `spark.gluten.velox.` prefixes and misspells it.
///
/// The helpers below close that gap by checking every key Gluten emits against
/// facebook::velox::core::QueryConfig::registeredProperties() and
/// facebook::velox::functions::sparksql::SparkQueryConfig::registeredProperties(),
/// which are enumerable and therefore survive Velox renames.

#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace gluten {

/// Velox query config keys Gluten writes that are owned by an optional Velox
/// component with its own registry, and so are absent from the QueryConfig and
/// SparkQueryConfig registries. Treated as known.
const std::unordered_set<std::string>& externalVeloxQueryConfigKeys();

/// Every Velox query config key the linked Velox recognizes: the union of the
/// QueryConfig registry, the SparkQueryConfig registry (qualified with
/// SparkQueryConfig::kPrefix, since both share one map), and
/// externalVeloxQueryConfigKeys(). Computed once on first call.
const std::unordered_set<std::string>& knownVeloxQueryConfigKeys();

/// The sorted subset of `configs` keys that the linked Velox does not
/// recognize. Empty means the whole map will take effect.
std::vector<std::string> unknownVeloxQueryConfigKeys(const std::unordered_map<std::string, std::string>& configs);

/// Reports keys the linked Velox does not recognize. Throws GlutenException
/// when `strict`, otherwise logs a warning naming each key.
void checkVeloxQueryConfigKeys(const std::unordered_map<std::string, std::string>& configs, bool strict);

} // namespace gluten
