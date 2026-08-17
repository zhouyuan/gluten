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
import org.apache.gluten.extension.joinagg.PushAggregateThroughJoin

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for the cardinality gate in [[PushAggregateThroughJoin]]: a pushed pre-aggregation is kept
 * only when it can be shown to remove rows.
 *
 * The fixtures are sized so the dimension row counts, which bound the distinct counts of the fact
 * table's foreign keys, either stay well below the fact row count (the pre-aggregation reduces) or
 * multiply past it (it does not). `profitabilityCheckMinRows` is lowered to 1 throughout, since the
 * check is otherwise skipped at these sizes.
 */
class PushAggregateThroughJoinGateSuite extends PlanTest with SharedSparkSession {
  private val joinAggregateRule = PushAggregateThroughJoin(spark)

  override protected def sparkConf: SparkConf = {
    // Avoid Janino projection codegen here because Spark 4's QueryExecutionErrors
    // has Arrow-typed methods, which breaks test runs as arrow-vector is excluded.
    super.sparkConf
      .set(SQLConf.CODEGEN_FACTORY_MODE.key, "NO_CODEGEN")
      .set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    import testImplicits._

    val dimKeys = 0 until 12
    // One fact row per (item, store, date) diagonal position, repeated, so that grouping by the
    // three foreign keys together cannot reduce much while grouping by any single one can.
    (0 until 120)
      .map(i => (i % 12, (i / 2) % 12, (i / 3) % 12, (i % 7).toDouble))
      .toDF("f_item_sk", "f_store_sk", "f_date_sk", "f_price")
      .createOrReplaceTempView("fact")

    dimKeys.map(k => (k, s"brand-${k % 3}")).toDF("i_item_sk", "i_brand")
      .createOrReplaceTempView("dim_item")
    dimKeys.map(k => (k, s"store-${k % 4}")).toDF("s_store_sk", "s_name")
      .createOrReplaceTempView("dim_store")
    dimKeys.map(k => (k, 1990 + k % 2)).toDF("d_date_sk", "d_year")
      .createOrReplaceTempView("dim_date")
  }

  override def afterAll(): Unit = {
    try {
      Seq("fact", "dim_item", "dim_store", "dim_date").foreach(spark.catalog.dropTempView)
    } finally {
      super.afterAll()
    }
  }

  private def withRule[T](f: => T): T = {
    val previous = spark.experimental.extraOptimizations
    try {
      spark.experimental.extraOptimizations = Seq[Rule[LogicalPlan]](joinAggregateRule)
      f
    } finally {
      spark.experimental.extraOptimizations = previous
    }
  }

  /**
   * Runs `sql` with the gate configured as given and returns how many join edges the pushed
   * aggregate crossed, how many pushes the gate rejected, and the rows produced.
   */
  private def run(
      sql: String,
      confs: (String, String)*): (Int, Int, Seq[Row], LogicalPlan) = {
    val allConfs = Seq(
      GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_ENABLED.key -> "true",
      GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_PROFITABILITY_CHECK_MIN_ROWS.key -> "1"
    ) ++ confs
    // `withSQLConf` is fixed to a Unit body, so hand the result out through a local.
    var result: (Int, Int, Seq[Row], LogicalPlan) = null
    withSQLConf(allConfs: _*) {
      withRule {
        joinAggregateRule.resetSuccessfulPushCount()
        joinAggregateRule.resetUnprofitablePushCount()
        val df = spark.sql(sql)
        val rows = df.collect().toSeq.sortBy(_.toString())
        result = (
          joinAggregateRule.getSuccessfulPushCount,
          joinAggregateRule.getUnprofitablePushCount,
          rows,
          df.queryExecution.optimizedPlan)
      }
    }
    result
  }

  private def expectedRows(sql: String): Seq[Row] = {
    val previous = spark.experimental.extraOptimizations
    try {
      spark.experimental.extraOptimizations = Nil
      spark.sql(sql).collect().toSeq.sortBy(_.toString())
    } finally {
      spark.experimental.extraOptimizations = previous
    }
  }

  private val starSchemaSql =
    """
      |SELECT i_brand, s_name, d_year, sum(f_price) AS total
      |FROM fact, dim_item, dim_store, dim_date
      |WHERE f_item_sk = i_item_sk AND f_store_sk = s_store_sk AND f_date_sk = d_date_sk
      |GROUP BY i_brand, s_name, d_year
      |""".stripMargin

  private val singleDimensionSql =
    """
      |SELECT i_brand, sum(f_price) AS total
      |FROM fact, dim_item
      |WHERE f_item_sk = i_item_sk
      |GROUP BY i_brand
      |""".stripMargin

  private val groupByJoinKeySql =
    """
      |SELECT i_item_sk, s_name, d_year, sum(f_price) AS total
      |FROM fact, dim_item, dim_store, dim_date
      |WHERE f_item_sk = i_item_sk AND f_store_sk = s_store_sk AND f_date_sk = d_date_sk
      |GROUP BY i_item_sk, s_name, d_year
      |""".stripMargin

  test("a pre-aggregation on the whole foreign key set is dropped as non-reducing") {
    // The TPC-DS q47 shape: every grouping key comes from a dimension, so the pushed aggregate has
    // to keep all three foreign keys and groups at close to fact-table grain.
    val (pushes, rejected, rows, plan) = run(starSchemaSql)
    assert(pushes == 0, s"expected no push, got:\n${plan.treeString}")
    assert(rejected == 1)
    assert(!plan.toString().contains("join_agg_wrapper_"))
    assert(rows == expectedRows(starSchemaSql))
  }

  test("a pre-aggregation that provably reduces is kept") {
    // Only one foreign key survives into the pushed grouping, and the dimension it joins to bounds
    // its distinct count well below the fact row count.
    val (pushes, rejected, rows, plan) = run(singleDimensionSql)
    assert(pushes == 1, s"expected one push, got:\n${plan.treeString}")
    assert(rejected == 0)
    assert(plan.toString().contains("join_agg_wrapper_"))
    assert(rows == expectedRows(singleDimensionSql))
  }

  test("grouping by a join key needs no cardinality estimate") {
    // `f_item_sk` is equi-join-equal to the `i_item_sk` the query groups by, so that key adds no
    // granularity. The other two foreign keys still do, so this stays gated.
    val (pushes, rejected, _, plan) = run(groupByJoinKeySql)
    assert(pushes == 0, s"expected no push, got:\n${plan.treeString}")
    assert(rejected == 1)
  }

  test("grouping only by join keys is always pushed") {
    val sql =
      """
        |SELECT i_item_sk, sum(f_price) AS total
        |FROM fact, dim_item
        |WHERE f_item_sk = i_item_sk
        |GROUP BY i_item_sk
        |""".stripMargin
    // Even with a reduction requirement no estimate could satisfy, the pushed aggregate groups at
    // exactly the grain the query groups at, so it is kept.
    val (pushes, rejected, rows, plan) = run(
      sql,
      GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_MIN_REDUCTION_RATIO.key -> "1000000")
    assert(pushes == 1, s"expected one push, got:\n${plan.treeString}")
    assert(rejected == 0)
    assert(rows == expectedRows(sql))
  }

  test("minReductionRatio <= 1 disables the gate") {
    val (pushes, rejected, rows, plan) = run(
      starSchemaSql,
      GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_MIN_REDUCTION_RATIO.key -> "1.0")
    assert(pushes == 3, s"expected three pushes, got:\n${plan.treeString}")
    assert(rejected == 0)
    assert(plan.toString().contains("join_agg_wrapper_"))
    assert(rows == expectedRows(starSchemaSql))
  }

  test("profitabilityCheckMinRows skips the gate for small inputs") {
    val (pushes, rejected, _, plan) = run(
      starSchemaSql,
      GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_PROFITABILITY_CHECK_MIN_ROWS.key -> "1000000")
    assert(pushes == 3, s"expected three pushes, got:\n${plan.treeString}")
    assert(rejected == 0)
  }

  /**
   * Optimizes `sql` without executing it, so the fixtures can be sized like a real warehouse.
   */
  private def optimizeOnly(sql: String, confs: (String, String)*): (Int, Int, LogicalPlan) = {
    var result: (Int, Int, LogicalPlan) = null
    withSQLConf(
      (Seq(GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_ENABLED.key -> "true") ++ confs): _*) {
      withRule {
        joinAggregateRule.resetSuccessfulPushCount()
        joinAggregateRule.resetUnprofitablePushCount()
        val plan = spark.sql(sql).queryExecution.optimizedPlan
        result = (
          joinAggregateRule.getSuccessfulPushCount,
          joinAggregateRule.getUnprofitablePushCount,
          plan)
      }
    }
    result
  }

  private def registerWarehouseScaleViews(): Unit = {
    spark
      .range(0, 28800000000L)
      .selectExpr(
        "id % 402000 AS ss_item_sk",
        "id % 1500 AS ss_store_sk",
        "id % 73049 AS ss_sold_date_sk",
        "id % 65000000 AS ss_customer_sk",
        "cast(id % 100 AS double) AS ss_sales_price"
      )
      .createOrReplaceTempView("big_store_sales")
    spark
      .range(0, 402000)
      .selectExpr(
        "id AS i_item_sk",
        "concat('b', id % 700) AS i_brand",
        "concat('c', id % 10) AS i_category")
      .createOrReplaceTempView("big_item")
    spark
      .range(0, 1500)
      .selectExpr("id AS s_store_sk", "concat('s', id) AS s_store_name")
      .createOrReplaceTempView("big_store")
    spark
      .range(0, 73049)
      .selectExpr("id AS d_date_sk", "1900 + id % 200 AS d_year", "1 + id % 12 AS d_moy")
      .createOrReplaceTempView("big_date_dim")
    spark
      .range(0, 65000000)
      .selectExpr("id AS c_customer_sk", "concat('n', id % 5000) AS c_first_name")
      .createOrReplaceTempView("big_customer")
  }

  test("the TPC-DS q47 shape is not pushed at warehouse scale with default settings") {
    registerWarehouseScaleViews()
    val sql =
      """
        |SELECT i_category, i_brand, s_store_name, d_year, d_moy, sum(ss_sales_price) AS sum_sales
        |FROM big_item, big_store_sales, big_date_dim, big_store
        |WHERE ss_item_sk = i_item_sk
        |  AND ss_sold_date_sk = d_date_sk
        |  AND ss_store_sk = s_store_sk
        |GROUP BY i_category, i_brand, s_store_name, d_year, d_moy
        |""".stripMargin
    // Pushing here would pre-aggregate by (ss_item_sk, ss_store_sk, ss_sold_date_sk), which is
    // close to a key of the fact table, and would displace the pre-shuffle partial aggregate that
    // does the real reduction.
    val (pushes, rejected, plan) = optimizeOnly(sql)
    assert(pushes == 0, s"expected no push, got:\n${plan.treeString}")
    assert(rejected == 1)
    assert(!plan.toString().contains("join_agg_wrapper_"))
  }

  test("a large-dimension aggregate is still pushed at warehouse scale with default settings") {
    registerWarehouseScaleViews()
    val sql =
      """
        |SELECT c_first_name, sum(ss_sales_price) AS sum_sales
        |FROM big_customer, big_store_sales
        |WHERE ss_customer_sk = c_customer_sk
        |GROUP BY c_first_name
        |""".stripMargin
    // One foreign key, bounded by the customer row count, which is two orders of magnitude below
    // the fact row count.
    val (pushes, rejected, plan) = optimizeOnly(sql)
    assert(pushes == 1, s"expected one push, got:\n${plan.treeString}")
    assert(rejected == 0)
    assert(plan.toString().contains("join_agg_wrapper_"))
  }

  test("a higher ratio requirement drops a marginal pre-aggregation") {
    val (pushes, rejected, _, plan) = run(
      singleDimensionSql,
      GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_MIN_REDUCTION_RATIO.key -> "1000")
    assert(pushes == 0, s"expected no push, got:\n${plan.treeString}")
    assert(rejected == 1)
  }
}
