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
package org.apache.gluten.duckdb;

/**
 * JNI bridge into libgluten_duckdb (C++). The native side executes a self-contained Substrait plan
 * (a single parquet ReadRel with local_files) through DuckDB's substrait extension
 * ({@code from_substrait}) and streams the result back through the Arrow C Data Interface.
 */
public final class DuckDBScanJniWrapper {
  private DuckDBScanJniWrapper() {}

  /**
   * Lists the top-level column names of a parquet file in physical order. The caller uses this to
   * compute the positional projection DuckDB's substrait consumer expects.
   */
  public static native String[] describeParquet(String path);

  /**
   * Opens a scan over one task's split.
   *
   * @param plan serialized {@code substrait.Plan} holding a single parquet ReadRel with local_files
   * @param threads threads of the scan's DuckDB instance (0 = DuckDB default)
   * @param memoryLimit DuckDB memory_limit of the scan's instance, e.g. "1GB" ("" = default)
   * @param substraitExtensionPath path of a locally built substrait extension ("" = install/load
   *     the community extension)
   * @return native scan handle
   */
  public static native long open(
      byte[] plan, long threads, String memoryLimit, String substraitExtensionPath);

  /**
   * Fetches the next batch. On success the exported {@code ArrowSchema}/{@code ArrowArray} structs
   * are moved into the given JVM-allocated shells (which the caller owns and must import/release).
   *
   * @return true if a batch was produced, false on end of stream
   */
  public static native boolean next(long handle, long cSchemaAddress, long cArrayAddress);

  /** Closes the scan and releases native resources. Safe to call with 0. */
  public static native void close(long handle);
}
