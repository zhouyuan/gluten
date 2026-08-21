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

# Experimental DuckDB-backed scan offload (see docs/developers/duckdb-scan.md).
#
# Builds libgluten_duckdb: a small JNI shim that executes a Substrait plan with
# DuckDB's substrait extension (from_substrait) and streams the result back to
# the JVM over the Arrow C Data Interface. DuckDB itself is built from source
# here with the parquet extension statically linked; the substrait extension is
# loaded at runtime (it needs vcpkg-provided protobuf packages to build, so
# statically linking it is left as a follow-up).

include(FetchContent)

set(GLUTEN_DUCKDB_VERSION
    "v1.4.1"
    CACHE STRING "DuckDB version to build against.")

# DuckDB is third-party code; don't apply Gluten's -Werror to it. This only
# affects the current directory scope and below.
string(REPLACE "-Werror" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

# The substrait consumer maps ReadRel.local_files to parquet_scan, so the
# parquet extension must be linked in.
set(BUILD_EXTENSIONS
    "parquet"
    CACHE STRING "" FORCE)
set(EXTENSION_STATIC_BUILD
    ON
    CACHE BOOL "" FORCE)
set(BUILD_SHELL
    FALSE
    CACHE BOOL "" FORCE)
set(BUILD_UNITTESTS
    FALSE
    CACHE BOOL "" FORCE)

FetchContent_Declare(
  duckdb
  GIT_REPOSITORY https://github.com/duckdb/duckdb.git
  GIT_TAG ${GLUTEN_DUCKDB_VERSION}
  GIT_SHALLOW TRUE)
FetchContent_MakeAvailable(duckdb)

find_package(JNI REQUIRED)

add_library(gluten_duckdb SHARED DuckDBScan.cc DuckDBScanJniWrapper.cc)
set_target_properties(
  gluten_duckdb
  PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
             CXX_VISIBILITY_PRESET hidden)
target_include_directories(
  gluten_duckdb PRIVATE ${JNI_INCLUDE_DIRS}
                        ${duckdb_SOURCE_DIR}/src/include)
target_link_libraries(gluten_duckdb PRIVATE duckdb_static)

if(BUILD_TESTS)
  add_executable(gluten_duckdb_test tests/DuckDBScanTest.cc)
  target_include_directories(
    gluten_duckdb_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
                               ${duckdb_SOURCE_DIR}/src/include)
  target_link_libraries(gluten_duckdb_test gluten_duckdb duckdb_static
                        GTest::gtest GTest::gtest_main)
  gtest_discover_tests(gluten_duckdb_test)
endif()
