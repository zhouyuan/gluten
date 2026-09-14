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
package org.apache.gluten.utils

import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, NullsFirst, SortOrder}
import org.apache.spark.sql.execution.{FilterExec, LocalTableScanExec, SortExec, SparkPlan}
import org.apache.spark.sql.types.IntegerType

import org.scalatest.funsuite.AnyFunSuite

/**
 * Proves the part of [[GlutenVanillaPlanView]] that could plausibly not work: that overriding the
 * [[org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper]] helpers really does redirect
 * the plan assertions an unmodified Spark test makes.
 *
 * The operator mapping is stubbed here -- `LocalTableScanExec` stands in for an offloaded operator
 * whose vanilla form is a `SortExec` -- so this suite needs neither a Gluten backend on the
 * classpath nor a native library. [[GlutenVanillaPlanViewSuite]] covers the real mapping.
 */
class GlutenVanillaPlanViewInterceptionSuite extends AnyFunSuite with GlutenVanillaPlanView {

  private val attr = AttributeReference("a", IntegerType)()
  private val sortOrder = Seq(SortOrder(attr, Ascending, NullsFirst, Seq.empty))
  private val offloaded: SparkPlan = LocalTableScanExec(Seq(attr), Nil)

  override protected def vanillaEquivalentOf(plan: SparkPlan): Option[SparkPlan] = plan match {
    case _: LocalTableScanExec => Some(SortExec(sortOrder, global = true, child = plan))
    case _ => None
  }

  test("a vanilla operator assertion matches the operator it was offloaded to") {
    assert(collect(offloaded) { case _: SortExec => 1 }.size === 1)
    assert(find(offloaded)(_.isInstanceOf[SortExec]).isDefined)
    assert(collectFirst(offloaded) { case s: SortExec => s.global }.contains(true))
  }

  test("fields of the vanilla operator are readable, not just its type") {
    assert(collect(offloaded) { case s: SortExec => s.sortOrder } === Seq(sortOrder))
  }

  test("an assertion on the offloaded operator itself keeps working") {
    assert(collect(offloaded) { case _: LocalTableScanExec => 1 }.size === 1)
  }

  test("an unrelated operator type still does not match") {
    assert(collect(offloaded) { case _: FilterExec => 1 }.isEmpty)
    assert(find(offloaded)(_.isInstanceOf[FilterExec]).isEmpty)
  }

  test("a plan with nothing to map behaves as before") {
    val plan = FilterExec(attr, LocalTableScanExec(Seq(attr), Nil))
    assert(collect(plan) { case _: FilterExec => 1 }.size === 1)
  }
}
