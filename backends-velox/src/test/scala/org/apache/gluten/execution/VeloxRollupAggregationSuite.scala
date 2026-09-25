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

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.internal.SQLConf

class VeloxRollupAggregationSuite extends VeloxWholeStageTransformerSuite {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "2")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set(GlutenConfig.MERGE_TWO_PHASES_ENABLED.key, "false")
      .set(VeloxConfig.VELOX_ROLLUP_AGGREGATION_ENABLED.key, "true")
  }

  private def checkRollup(df: DataFrame): Unit = {
    val plan = getExecutedPlan(df)
    assert(plan.exists(_.isInstanceOf[RollupHashAggregateExecTransformer]), plan)
    assert(!plan.exists(_.isInstanceOf[ExpandExecTransformer]), plan)
  }

  private def checkNoRollup(df: DataFrame): Unit = {
    val plan = getExecutedPlan(df)
    assert(!plan.exists(_.isInstanceOf[RollupHashAggregateExecTransformer]), plan)
    assert(plan.exists(_.isInstanceOf[ExpandExecTransformer]), plan)
  }

  test("rollup") {
    runQueryAndCompare("""
                         |select l_returnflag, l_linestatus, l_shipmode, grouping_id(),
                         |  sum(l_quantity), count(*), count(l_comment), avg(l_extendedprice),
                         |  min(l_shipdate), max(l_comment), sum(l_linenumber), avg(l_linenumber)
                         |from lineitem
                         |group by rollup(l_returnflag, l_linestatus, l_shipmode)
                         |""".stripMargin)(checkRollup)
  }

  test("rollup over decimal aggregates and pre-projection") {
    runQueryAndCompare("""
                         |select l_returnflag, l_linestatus, l_shipmode,
                         |  sum(coalesce(cast(l_extendedprice as decimal(12, 2)) *
                         |    cast(l_discount as decimal(12, 2)), 0.00)),
                         |  avg(cast(l_quantity as decimal(12, 2))),
                         |  sum(l_linenumber * 2 + 1)
                         |from lineitem
                         |group by rollup(l_returnflag, l_linestatus, l_shipmode)
                         |""".stripMargin)(checkRollup)
  }

  test("grouping sets forming a chain") {
    runQueryAndCompare("""
                         |select l_returnflag, l_linestatus, l_shipmode, grouping_id(),
                         |  sum(l_quantity), count(*)
                         |from lineitem
                         |group by grouping sets (
                         |  (l_shipmode, l_returnflag, l_linestatus),
                         |  (l_shipmode, l_linestatus),
                         |  (l_linestatus))
                         |""".stripMargin)(checkRollup)
    runQueryAndCompare("""
                         |select l_returnflag, l_linestatus, l_shipmode, sum(l_quantity)
                         |from lineitem
                         |group by l_returnflag, rollup(l_linestatus, l_shipmode)
                         |""".stripMargin)(checkRollup)
  }

  test("aggregate reads a grouping key") {
    runQueryAndCompare("""
                         |select l_linenumber, l_returnflag, sum(l_linenumber), count(l_returnflag),
                         |  max(l_returnflag)
                         |from lineitem
                         |group by rollup(l_linenumber, l_returnflag)
                         |""".stripMargin)(checkRollup)
  }

  test("rollup with abandoned grouping sets") {
    // Every grouping set gives up aggregating and passes its input through as partial results.
    withSQLConf(
      VeloxConfig.ABANDON_PARTIAL_AGGREGATION_MIN_ROWS.key -> "10",
      VeloxConfig.ABANDON_PARTIAL_AGGREGATION_MIN_PCT.key -> "0") {
      runQueryAndCompare("""
                           |select l_orderkey, l_returnflag, l_linestatus,
                           |  sum(l_quantity), count(*), avg(l_discount)
                           |from lineitem
                           |group by rollup(l_returnflag, l_linestatus, l_orderkey)
                           |""".stripMargin)(checkRollup)
    }
  }

  test("rollup over empty input") {
    // Keep AQE from replacing the plan over the empty input with an empty relation.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      runQueryAndCompare("""
                           |select l_returnflag, l_linestatus, sum(l_quantity), count(*)
                           |from lineitem
                           |where l_orderkey < 0
                           |group by rollup(l_returnflag, l_linestatus)
                           |""".stripMargin)(checkRollup)
    }
  }

  test("unsupported grouping sets and aggregates") {
    // CUBE doesn't form a chain.
    runQueryAndCompare("""
                         |select l_returnflag, l_linestatus, sum(l_quantity)
                         |from lineitem
                         |group by cube(l_returnflag, l_linestatus)
                         |""".stripMargin)(checkNoRollup)
    runQueryAndCompare("""
                         |select l_returnflag, l_linestatus, sum(l_quantity)
                         |from lineitem
                         |group by grouping sets ((l_returnflag), (l_linestatus))
                         |""".stripMargin)(checkNoRollup)
    runQueryAndCompare("""
                         |select l_returnflag, l_linestatus, stddev(l_quantity)
                         |from lineitem
                         |group by rollup(l_returnflag, l_linestatus)
                         |""".stripMargin)(checkNoRollup)
    runQueryAndCompare("""
                         |select l_returnflag, l_linestatus, count(distinct l_partkey)
                         |from lineitem
                         |group by rollup(l_returnflag, l_linestatus)
                         |""".stripMargin) {
      df => assert(!getExecutedPlan(df).exists(_.isInstanceOf[RollupHashAggregateExecTransformer]))
    }
  }

  test("disabled by config") {
    withSQLConf(VeloxConfig.VELOX_ROLLUP_AGGREGATION_ENABLED.key -> "false") {
      runQueryAndCompare("""
                           |select l_returnflag, l_linestatus, sum(l_quantity)
                           |from lineitem
                           |group by rollup(l_returnflag, l_linestatus)
                           |""".stripMargin)(checkNoRollup)
    }
  }
}
