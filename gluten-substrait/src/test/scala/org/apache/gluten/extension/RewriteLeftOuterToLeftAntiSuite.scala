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
package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class RewriteLeftOuterToLeftAntiSuite extends SharedSparkSession {

  private val rule = RewriteLeftOuterToLeftAntiBatch(spark)

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

    // Order 5 carries a null order number on both sides, so an `=` join leaves it unmatched while a
    // `<=>` join matches it. That is what separates the two join conditions in the tests below.
    Seq[(Option[Int], Int, Int, Int, Int)](
      (Some(1), 10, 5, 1000, 1),
      (Some(2), 20, 7, 1000, 2),
      (Some(3), 30, 9, 1001, 3),
      (Some(4), 40, 11, 1001, 4),
      (None, 50, 13, 1000, 5)
    ).toDF(
      "ws_order_number",
      "ws_item_sk",
      "ws_quantity",
      "ws_sold_date_sk",
      "ws_bill_customer_sk")
      .createOrReplaceTempView("web_sales")

    // wr_item_sk is non-nullable; wr_reason_sk is nullable and never a join key, and order 3 is a
    // matched row that carries a null there. That row is what an unguarded rewrite would lose.
    // Order 1 is returned twice, so the outer join multiplies it into two rows that the filter has
    // to discard, where the anti join emits none.
    Seq[(Option[Int], Int, Option[Int])](
      (Some(1), 10, Some(100)),
      (Some(1), 10, Some(101)),
      (Some(3), 30, None),
      (None, 50, Some(102))
    ).toDF("wr_order_number", "wr_item_sk", "wr_reason_sk")
      .createOrReplaceTempView("web_returns")

    Seq((1000, 2000), (1001, 2001))
      .toDF("d_date_sk", "d_year")
      .createOrReplaceTempView("date_dim")
  }

  override def afterAll(): Unit = {
    try {
      spark.catalog.dropTempView("web_sales")
      spark.catalog.dropTempView("web_returns")
      spark.catalog.dropTempView("date_dim")
    } finally {
      super.afterAll()
    }
  }

  private def withRule[T](f: => T): T = {
    val previous = spark.experimental.extraOptimizations
    try {
      spark.experimental.extraOptimizations = Seq(rule)
      f
    } finally {
      spark.experimental.extraOptimizations = previous
    }
  }

  private def joinTypesOf(plan: LogicalPlan): Seq[JoinType] = plan.collect {
    case join: Join => join.joinType
  }

  /** How many `null AS <right side column>` aliases the rewrite left behind unpruned. */
  private def nullAliasCount(plan: LogicalPlan): Int = plan
    .collect {
      case project: Project =>
        project.projectList.count {
          case Alias(literal: Literal, _) => literal.value == null
          case _ => false
        }
    }
    .sum

  private def collectSorted(sql: String): Seq[Row] =
    spark.sql(sql).collect().toSeq.sortBy(_.toString())

  /**
   * Runs `sql` with and without the rule, asserting the rewritten plan is well formed, joins the
   * way `expectedJoinTypes` says, and returns the very same rows as vanilla Spark.
   * `expectedNullAliases` pins how many of the rewrite's null projections survive the cleanup pass;
   * pass None where that is not the point of the test.
   */
  private def check(
      sql: String,
      expectedJoinTypes: Seq[JoinType],
      expectedNullAliases: Option[Int] = Some(0)): Unit = {
    val baseline = collectSorted(sql)
    withRule {
      val df = spark.sql(sql)
      val optimized = df.queryExecution.optimizedPlan
      assert(
        optimized.resolved && optimized.missingInput.isEmpty,
        s"Rewritten plan is not well formed, missingInput=${optimized.missingInput}:\n" +
          optimized.treeString
      )
      assert(
        joinTypesOf(optimized) == expectedJoinTypes,
        s"Unexpected join types:\n${optimized.treeString}")
      expectedNullAliases.foreach {
        expected =>
          assert(
            nullAliasCount(optimized) == expected,
            s"Unexpected number of null projections:\n${optimized.treeString}")
      }
      assert(
        df.collect().toSeq.sortBy(_.toString()) == baseline,
        s"Rewrite changed the result of:\n$sql")
    }
  }

  test("is-null check on an equi-join key becomes a left anti join") {
    val sql =
      """
        |SELECT ws_order_number, ws_quantity
        |FROM web_sales
        |LEFT JOIN web_returns
        |  ON ws_order_number = wr_order_number AND ws_item_sk = wr_item_sk
        |WHERE wr_order_number IS NULL
        |""".stripMargin
    check(sql, expectedJoinTypes = Seq(LeftAnti))
    assert(collectSorted(sql) == Seq(Row(2, 7), Row(4, 11), Row(null, 13)))
  }

  test("is-null check on a non-nullable right side column becomes a left anti join") {
    // wr_item_sk is not a join key here, but it is non-nullable in web_returns, so it can only be
    // null on a row the outer join null-extended.
    check(
      """
        |SELECT ws_order_number
        |FROM web_sales
        |LEFT JOIN web_returns ON ws_order_number = wr_order_number
        |WHERE wr_item_sk IS NULL
        |""".stripMargin,
      expectedJoinTypes = Seq(LeftAnti)
    )
  }

  test("is-null check on a nullable non-key column is not rewritten") {
    // Order 3 matches and still has a null wr_reason_sk, so it has to survive the filter. An anti
    // join would drop it.
    val sql =
      """
        |SELECT ws_order_number
        |FROM web_sales
        |LEFT JOIN web_returns
        |  ON ws_order_number = wr_order_number AND ws_item_sk = wr_item_sk
        |WHERE wr_reason_sk IS NULL
        |""".stripMargin
    check(sql, expectedJoinTypes = Seq(LeftOuter))
    assert(collectSorted(sql) == Seq(Row(2), Row(3), Row(4), Row(null)))
  }

  test("null-safe equi-join condition is not rewritten") {
    // `<=>` matches null against null, so order 5 is a matched row whose wr_order_number is null.
    val sql =
      """
        |SELECT ws_order_number
        |FROM web_sales
        |LEFT JOIN web_returns
        |  ON ws_order_number <=> wr_order_number AND ws_item_sk <=> wr_item_sk
        |WHERE wr_order_number IS NULL
        |""".stripMargin
    check(sql, expectedJoinTypes = Seq(LeftOuter))
    assert(collectSorted(sql) == Seq(Row(2), Row(4), Row(null)))
  }

  test("filter conjuncts that are not the unmatched check are preserved") {
    // The OR spans both sides so Spark cannot push it below the join, which leaves it above the
    // rewritten anti join reading wr_reason_sk from the null projection.
    check(
      """
        |SELECT ws_order_number
        |FROM web_sales
        |LEFT JOIN web_returns
        |  ON ws_order_number = wr_order_number AND ws_item_sk = wr_item_sk
        |WHERE wr_order_number IS NULL AND (ws_quantity > 8 OR wr_reason_sk IS NULL)
        |""".stripMargin,
      expectedJoinTypes = Seq(LeftAnti),
      expectedNullAliases = None
    )
  }

  test("right side columns read above the filter become nulls") {
    val sql =
      """
        |SELECT ws_order_number, wr_reason_sk
        |FROM web_sales
        |LEFT JOIN web_returns
        |  ON ws_order_number = wr_order_number AND ws_item_sk = wr_item_sk
        |WHERE wr_order_number IS NULL
        |""".stripMargin
    check(sql, expectedJoinTypes = Seq(LeftAnti), expectedNullAliases = Some(1))
    assert(collectSorted(sql) == Seq(Row(2, null), Row(4, null), Row(null, null)))
  }

  test("TPC-DS q78 shape: filter is pushed between the inner join and the outer join") {
    // The `WHERE wr_order_number IS NULL` conjunct lands directly above the outer join once Spark
    // pushes it below the date_dim join, which is the shape the rule has to recognise in q78.
    val sql =
      """
        |SELECT d_year, ws_item_sk, ws_bill_customer_sk, sum(ws_quantity) AS ws_qty
        |FROM web_sales
        |LEFT JOIN web_returns
        |  ON wr_order_number = ws_order_number AND ws_item_sk = wr_item_sk
        |JOIN date_dim ON ws_sold_date_sk = d_date_sk
        |WHERE wr_order_number IS NULL
        |GROUP BY d_year, ws_item_sk, ws_bill_customer_sk
        |""".stripMargin
    check(sql, expectedJoinTypes = Seq(Inner, LeftAnti))
    assert(
      collectSorted(sql) == Seq(Row(2000, 20, 2, 7), Row(2000, 50, 5, 13), Row(2001, 40, 4, 11)))
  }

  test("rewrite can be turned off") {
    withSQLConf(GlutenConfig.REWRITE_LEFT_OUTER_TO_LEFT_ANTI_ENABLED.key -> "false") {
      check(
        """
          |SELECT ws_order_number
          |FROM web_sales
          |LEFT JOIN web_returns
          |  ON ws_order_number = wr_order_number AND ws_item_sk = wr_item_sk
          |WHERE wr_order_number IS NULL
          |""".stripMargin,
        expectedJoinTypes = Seq(LeftOuter)
      )
    }
  }
}
