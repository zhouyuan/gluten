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

#include "config/VeloxConfigContract.h"

#include <algorithm>
#include <sstream>

#include <glog/logging.h>

#include "utils/Exception.h"
#include "velox/core/QueryConfig.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"

#ifdef GLUTEN_ENABLE_GPU
#include "velox/experimental/cudf/CudfConfig.h"
#endif

namespace gluten {

namespace {

using facebook::velox::core::QueryConfig;
using facebook::velox::functions::sparksql::SparkQueryConfig;

std::unordered_set<std::string> buildKnownVeloxQueryConfigKeys() {
  std::unordered_set<std::string> keys;
  for (const auto& property : QueryConfig::registeredProperties()) {
    keys.insert(property.name);
  }
  // SparkQueryConfig shares the QueryConfig map and disambiguates its own
  // properties with kPrefix, so compare against the qualified form -- that is
  // what writers put in the map.
  for (const auto& property : SparkQueryConfig::registeredProperties()) {
    keys.insert(SparkQueryConfig::qualify(property.name));
  }
  for (const auto& key : externalVeloxQueryConfigKeys()) {
    keys.insert(key);
  }
  return keys;
}

} // namespace

const std::unordered_set<std::string>& externalVeloxQueryConfigKeys() {
  static const std::unordered_set<std::string> kKeys = {
#ifdef GLUTEN_ENABLE_GPU
      std::string(facebook::velox::cudf_velox::CudfConfig::kCudfEnabled),
#endif
  };
  return kKeys;
}

const std::unordered_set<std::string>& knownVeloxQueryConfigKeys() {
  static const std::unordered_set<std::string> kKeys = buildKnownVeloxQueryConfigKeys();
  return kKeys;
}

std::vector<std::string> unknownVeloxQueryConfigKeys(const std::unordered_map<std::string, std::string>& configs) {
  const auto& known = knownVeloxQueryConfigKeys();
  std::vector<std::string> unknown;
  for (const auto& config : configs) {
    if (known.find(config.first) == known.end()) {
      unknown.push_back(config.first);
    }
  }
  // Sorted so the diagnostic is stable across runs.
  std::sort(unknown.begin(), unknown.end());
  return unknown;
}

void checkVeloxQueryConfigKeys(const std::unordered_map<std::string, std::string>& configs, bool strict) {
  const auto unknown = unknownVeloxQueryConfigKeys(configs);
  if (unknown.empty()) {
    return;
  }

  std::ostringstream oss;
  oss << "The linked Velox does not recognize " << unknown.size() << " of the query config key(s) Gluten set, "
      << "so they will be silently ignored: ";
  for (size_t i = 0; i < unknown.size(); ++i) {
    if (i > 0) {
      oss << ", ";
    }
    oss << unknown[i];
  }
  oss << ". A Velox update most likely renamed, removed or deregistered these properties. "
      << "If a key was passed through a Gluten backend conf prefix, check it for typos.";

  if (strict) {
    throw GlutenException(oss.str());
  }
  LOG(WARNING) << oss.str();
}

} // namespace gluten
