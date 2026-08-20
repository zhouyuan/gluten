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

import org.apache.gluten.config.GlutenDataFusionConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame

/**
 * End-to-end tests for DataFusion-backed scan offload. Requires libgluten_datafusion, built with
 * `dev/builddeps-veloxbe.sh --enable_datafusion=ON` (or `cargo build --release` under rust/ with
 * the artifact copied into cpp/build/releases).
 */
class DataFusionScanSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  import testImplicits._

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.unsafe.exceptionOnMemoryLeak", "true")
    .set(GlutenDataFusionConfig.SCAN_ENABLED.key, "true")

  private def collectDataFusionScans(df: DataFrame): Seq[DataFusionScanExec] = {
    df.queryExecution.executedPlan.collect { case scan: DataFusionScanExec => scan }
  }

  private def withTestTable(testFun: => Unit): Unit = {
    withTempPath {
      path =>
        (0 until 1000)
          .map(i => (i.toLong, i % 7, s"name-${i % 13}", (i % 100) / 4.0))
          .toDF("id", "bucket", "name", "price")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("df_tbl")
        testFun
    }
  }

  test("projection-only scan is offloaded to DataFusion and matches vanilla Spark") {
    withTestTable {
      runQueryAndCompare("select name, id from df_tbl") {
        df =>
          val scans = collectDataFusionScans(df)
          assert(scans.length == 1, s"plan: ${df.queryExecution.executedPlan}")
      }
    }
  }

  test("filter and aggregate above the scan run in a Velox whole stage") {
    withTestTable {
      runQueryAndCompare(
        "select bucket, count(*) as cnt, sum(price) from df_tbl where id > 100 group by bucket") {
        df =>
          val plan = df.queryExecution.executedPlan
          assert(collectDataFusionScans(df).length == 1, s"plan: $plan")
          val wholeStages = plan.collect { case w: WholeStageTransformer => w }
          assert(wholeStages.nonEmpty, s"plan: $plan")
          // The filter must not be lost: it runs natively on Velox.
          val veloxFilters = plan.collect { case f: FilterExecTransformerBase => f }
          assert(veloxFilters.nonEmpty, s"plan: $plan")
      }
    }
  }

  test("partitioned table including null partition values") {
    withTempPath {
      path =>
        spark
          .range(0, 100)
          .selectExpr("id", "if(id % 3 = 0, null, concat('p-', id % 3)) as part")
          .write
          .partitionBy("part")
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("df_part_tbl")
        runQueryAndCompare("select part, id from df_part_tbl") {
          df => assert(collectDataFusionScans(df).length == 1)
        }
        runQueryAndCompare("select count(*) as cnt, part from df_part_tbl group by part") {
          df => assert(collectDataFusionScans(df).length == 1)
        }
    }
  }

  test("small file split sizes keep results exactly-once") {
    withTestTable {
      withSQLConf("spark.sql.files.maxPartitionBytes" -> "2048") {
        runQueryAndCompare("select count(*) as cnt, sum(id) as s from df_tbl") {
          df => assert(collectDataFusionScans(df).length == 1)
        }
      }
    }
  }

  test("tpch q6-shaped query") {
    createTPCHNotNullTables()
    runQueryAndCompare("""
                         |select sum(l_extendedprice * l_discount) as revenue
                         |from lineitem
                         |where l_shipdate >= '1994-01-01'
                         |  and l_shipdate < '1995-01-01'
                         |  and l_discount between 0.05 and 0.07
                         |  and l_quantity < 24
                         |""".stripMargin) {
      df => assert(collectDataFusionScans(df).length == 1)
    }
  }

  test("unsupported scans fall back to the Velox scan") {
    withTestTable {
      // Complex type in output.
      withTempPath {
        path =>
          spark
            .range(10)
            .selectExpr("id", "array(id, id + 1) as arr")
            .write
            .parquet(path.getCanonicalPath)
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("df_complex_tbl")
          runQueryAndCompare("select id, arr from df_complex_tbl") {
            df =>
              assert(collectDataFusionScans(df).isEmpty)
              val veloxScans = df.queryExecution.executedPlan.collect {
                case scan: FileSourceScanExecTransformer => scan
              }
              assert(veloxScans.length == 1, s"plan: ${df.queryExecution.executedPlan}")
          }
      }
    }
  }

  test("config off keeps the Velox scan") {
    withTestTable {
      withSQLConf(GlutenDataFusionConfig.SCAN_ENABLED.key -> "false") {
        runQueryAndCompare("select name, id from df_tbl") {
          df =>
            assert(collectDataFusionScans(df).isEmpty)
            val veloxScans = df.queryExecution.executedPlan.collect {
              case scan: FileSourceScanExecTransformer => scan
            }
            assert(veloxScans.length == 1)
        }
      }
    }
  }
}
