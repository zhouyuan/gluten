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

import org.apache.gluten.execution.{ProjectExecTransformer, SortExecTransformer}

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Literal, SortOrder}
import org.apache.spark.sql.catalyst.expressions.{Ascending, NullsFirst}
import org.apache.spark.sql.execution.{FilterExec, LocalTableScanExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.types.IntegerType

import org.scalatest.funsuite.AnyFunSuite

/**
 * Shows that a plan assertion written against vanilla Spark holds for the offloaded plan, which is
 * why a Spark test that only checks the operator type does not need to be copied into gluten-ut and
 * edited.
 *
 * Builds the operators directly, so no SparkSession and no native library are needed. It does need
 * a backend jar on the classpath, because constructing a transformer triggers Gluten's component
 * discovery; [[GlutenVanillaPlanViewInterceptionSuite]] covers the same ground with a stubbed
 * mapping and no backend at all.
 */
class GlutenVanillaPlanViewSuite extends AnyFunSuite with GlutenVanillaPlanView {

  private val attr = AttributeReference("a", IntegerType)()
  private val leaf: SparkPlan = LocalTableScanExec(Seq(attr), Nil)
  private val sortOrder = Seq(SortOrder(attr, Ascending, NullsFirst, Seq.empty))

  test("a vanilla operator assertion matches the operator Gluten offloaded it to") {
    val plan = SortExecTransformer(sortOrder, global = true, child = leaf)

    // What an unmodified Spark test does. Fails without the shim.
    assert(collect(plan) { case _: SortExec => 1 }.size === 1)
    assert(find(plan)(_.isInstanceOf[SortExec]).isDefined)
    assert(collectFirst(plan) { case s: SortExec => s.global }.contains(true))
  }

  test("fields of the vanilla operator are readable, not just its type") {
    val plan = ProjectExecTransformer(Seq(Alias(Literal(1), "one")()), leaf)
    assert(collect(plan) { case p: ProjectExec => p.projectList.map(_.name) } === Seq(Seq("one")))
  }

  test("assertions on the Gluten operator keep working unchanged") {
    val plan = SortExecTransformer(sortOrder, global = true, child = leaf)
    assert(collect(plan) { case _: SortExecTransformer => 1 }.size === 1)
  }

  test("an unrelated operator type still does not match") {
    val plan = SortExecTransformer(sortOrder, global = true, child = leaf)
    assert(collect(plan) { case _: FilterExec => 1 }.isEmpty)
    assert(find(plan)(_.isInstanceOf[FilterExec]).isEmpty)
  }

  test("a plan with no Gluten operators behaves as before") {
    val plan = SortExec(sortOrder, global = false, child = leaf)
    assert(collect(plan) { case _: SortExec => 1 }.size === 1)
    assert(collect(plan) { case _: SortExecTransformer => 1 }.isEmpty)
  }
}
