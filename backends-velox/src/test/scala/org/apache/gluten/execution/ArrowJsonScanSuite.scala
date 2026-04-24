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
package org.apache.gluten.execution

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.datasource.ArrowJSONFileFormat
import org.apache.gluten.datasource.v2.ArrowJSONScan

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

class ArrowJsonScanSuite extends VeloxWholeStageTransformerSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.NATIVE_ARROW_READER_ENABLED.key, "true")
  }

  test("test arrow json file format") {
    withTempPath {
      path =>
        val data = spark.range(10).selectExpr("id", "id * 2 as value")
        data.write.json(path.getCanonicalPath)

        val df = spark.read.json(path.getCanonicalPath)
        val plan = df.queryExecution.executedPlan

        // Check if ArrowJSONFileFormat is used
        val fileSourceScans = plan.collect { case f: FileSourceScanExec => f }
        assert(
          fileSourceScans.exists(
            _.relation.fileFormat
              .isInstanceOf[ArrowJSONFileFormat]))
    }
  }

  test("test arrow json scan v2") {
    withTempPath {
      path =>
        val data = spark.range(10).selectExpr("id", "id * 2 as value")
        data.write.json(path.getCanonicalPath)

        val df = spark.read.format("json").load(path.getCanonicalPath)
        val plan = df.queryExecution.executedPlan

        // Check if ArrowJSONScan is used
        val batchScans = plan.collect { case b: BatchScanExec => b }
        assert(batchScans.exists(_.scan.isInstanceOf[ArrowJSONScan]))
    }
  }

  test("test arrow json read and query") {
    withTempPath {
      path =>
        val data = spark.range(100).selectExpr("id", "id * 2 as value", "cast(id as string) as name")
        data.write.json(path.getCanonicalPath)

        val df = spark.read.json(path.getCanonicalPath)
        val result = df.filter("id > 50").selectExpr("id", "value").collect()
        
        assert(result.length == 49)
        assert(result.head.getLong(0) == 51)
        assert(result.head.getLong(1) == 102)
    }
  }
}

// Made with Bob
