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
package org.apache.gluten.config

import org.apache.spark.sql.internal.SQLConf

class GlutenDuckDBConfig(conf: SQLConf) extends GlutenCoreConfig(conf) {
  import GlutenDuckDBConfig._

  def scanEnabled: Boolean = getConf(SCAN_ENABLED)

  def scanTimestampEnabled: Boolean = getConf(SCAN_TIMESTAMP_ENABLED)

  def threads: Int = getConf(THREADS)

  def memoryLimit: String = getConf(MEMORY_LIMIT)

  def substraitExtensionPath: String = getConf(SUBSTRAIT_EXTENSION_PATH)
}

object GlutenDuckDBConfig extends ConfigRegistry {

  def get: GlutenDuckDBConfig = {
    new GlutenDuckDBConfig(SQLConf.get)
  }

  val SCAN_ENABLED: ConfigEntry[Boolean] =
    buildConf("spark.gluten.sql.columnar.duckdb.scan.enabled")
      .doc(
        "Experimental: Offload plain parquet file scans to DuckDB via its substrait extension." +
          " Downstream operators keep running on the Velox backend, consuming the scan output as" +
          " Arrow batches. Scans that DuckDB cannot handle fall back to the regular Velox scan.")
      .booleanConf
      .createWithDefault(false)

  val SCAN_TIMESTAMP_ENABLED: ConfigEntry[Boolean] =
    buildConf("spark.gluten.sql.columnar.duckdb.scan.timestampEnabled")
      .doc(
        "Experimental: Allow timestamp columns in DuckDB-offloaded scans. Disabled by default" +
          " until the timezone semantics of the Arrow-to-Velox import are validated.")
      .booleanConf
      .createWithDefault(false)

  val THREADS: ConfigEntry[Int] =
    buildConf("spark.gluten.sql.columnar.duckdb.threads")
      .doc(
        "Threads of each scan's DuckDB instance. One instance serves one Spark task, so this" +
          " defaults to a single thread. 0 means the DuckDB default (one thread per core), which" +
          " is rarely what you want with many concurrent tasks.")
      .intConf
      .createWithDefault(1)

  val MEMORY_LIMIT: ConfigEntry[String] =
    buildConf("spark.gluten.sql.columnar.duckdb.memoryLimit")
      .doc(
        "DuckDB memory_limit of each scan's instance, e.g. '1GB'. Note this is per concurrent" +
          " task and is not accounted to Spark's memory manager. Empty keeps the DuckDB default.")
      .stringConf
      .createWithDefault("")

  val SUBSTRAIT_EXTENSION_PATH: ConfigEntry[String] =
    buildConf("spark.gluten.sql.columnar.duckdb.substraitExtensionPath")
      .doc(
        "Path of a locally built DuckDB substrait extension available on every node. When empty," +
          " the community extension is installed and loaded instead (INSTALL substrait FROM" +
          " community), which needs network access once per host.")
      .stringConf
      .createWithDefault("")
}
