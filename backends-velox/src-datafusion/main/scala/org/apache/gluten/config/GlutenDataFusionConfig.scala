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

class GlutenDataFusionConfig(conf: SQLConf) extends GlutenCoreConfig(conf) {
  import GlutenDataFusionConfig._

  def scanEnabled: Boolean = getConf(SCAN_ENABLED)

  def scanTimestampEnabled: Boolean = getConf(SCAN_TIMESTAMP_ENABLED)

  def threads: Int = getConf(THREADS)
}

object GlutenDataFusionConfig extends ConfigRegistry {

  def get: GlutenDataFusionConfig = {
    new GlutenDataFusionConfig(SQLConf.get)
  }

  val SCAN_ENABLED: ConfigEntry[Boolean] =
    buildConf("spark.gluten.sql.columnar.datafusion.scan.enabled")
      .doc(
        "Experimental: Offload plain parquet file scans to Apache DataFusion. Downstream" +
          " operators keep running on the Velox backend, consuming the scan output as Arrow" +
          " batches. Scans that DataFusion cannot handle fall back to the regular Velox scan.")
      .booleanConf
      .createWithDefault(false)

  val SCAN_TIMESTAMP_ENABLED: ConfigEntry[Boolean] =
    buildConf("spark.gluten.sql.columnar.datafusion.scan.timestampEnabled")
      .doc(
        "Experimental: Allow timestamp columns in DataFusion-offloaded scans. Disabled by" +
          " default until the timezone semantics of the Arrow-to-Velox import are validated.")
      .booleanConf
      .createWithDefault(false)

  val THREADS: ConfigEntry[Int] =
    buildConf("spark.gluten.sql.columnar.datafusion.threads")
      .doc(
        "Number of threads of the process-global DataFusion tokio runtime shared by all" +
          " concurrent scans in an executor. 0 means one thread per core. Only the value seen" +
          " by the first scan of the process takes effect.")
      .intConf
      .createWithDefault(0)
}
