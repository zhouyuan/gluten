#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds velox4j and gluten-flink, producing a single bundle jar:
#   gluten-flink/bundle/target/gluten-flink-bundle-<version>.jar
#
# Options:
#   --velox4j_home=<dir>   Use an existing velox4j checkout instead of cloning one.
#   --build_velox4j=OFF    Skip building velox4j and reuse the one in the local maven repo.

set -eux

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_FLINK_DIR="$CURRENT_DIR/.."
GLUTEN_DIR="$GLUTEN_FLINK_DIR/.."
MVN="$GLUTEN_DIR/build/mvn"

# Keep in sync with .github/workflows/flink.yml and gluten-flink/docs/Flink.md.
VELOX4J_REPO=${VELOX4J_REPO:-https://github.com/bigo-sg/velox4j.git}
VELOX4J_BRANCH=${VELOX4J_BRANCH:-gluten-20260829}
VELOX4J_COMMIT=${VELOX4J_COMMIT:-26c7715278e6f6795084f6334998cb2ce382f7aa}
VELOX4J_HOME=""
BUILD_VELOX4J=ON

for arg in "$@"; do
  case $arg in
    --velox4j_home=*)
      VELOX4J_HOME="${arg#*=}"
      ;;
    --build_velox4j=*)
      BUILD_VELOX4J="${arg#*=}"
      ;;
    *)
      echo "Unknown option: $arg"
      exit 1
      ;;
  esac
done

# Same native dependency settings as CI.
export VELOX_DEPENDENCY_SOURCE=${VELOX_DEPENDENCY_SOURCE:-BUNDLED}
export fmt_SOURCE=${fmt_SOURCE:-BUNDLED}
export folly_SOURCE=${folly_SOURCE:-BUNDLED}

if [ "$BUILD_VELOX4J" = "ON" ]; then
  if [ -z "$VELOX4J_HOME" ]; then
    VELOX4J_HOME="$GLUTEN_DIR/ep/_ep/velox4j"
    if [ ! -d "$VELOX4J_HOME/.git" ]; then
      mkdir -p "$(dirname "$VELOX4J_HOME")"
      git clone -b "$VELOX4J_BRANCH" "$VELOX4J_REPO" "$VELOX4J_HOME"
    fi
    cd "$VELOX4J_HOME"
    git fetch origin "$VELOX4J_BRANCH"
    git reset --hard "$VELOX4J_COMMIT"
    git apply "$GLUTEN_FLINK_DIR/patches/fix-velox4j.patch"
  fi
  cd "$VELOX4J_HOME"
  "$MVN" clean install -DskipTests -Dgpg.skip -Dspotless.skip=true
fi

# Build only what the bundle needs, so the ut module's nexmark dependency is not required.
cd "$GLUTEN_FLINK_DIR"
"$MVN" clean package -pl bundle -am -Dmaven.test.skip=true

ls -l "$GLUTEN_FLINK_DIR"/bundle/target/gluten-flink-bundle-*.jar
