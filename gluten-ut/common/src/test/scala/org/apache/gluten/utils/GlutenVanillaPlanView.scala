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

import org.apache.gluten.execution.{FilterExecTransformerBase, ProjectExecTransformerBase, SortExecTransformer, WindowExecTransformer}

import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.window.WindowExec

/**
 * The offloaded counterpart of a vanilla Spark operator, for plan assertions only.
 *
 * A large share of the Spark tests Gluten has to copy into `gluten-ut` and edit are copied for one
 * reason: the test asserts on the operator type, and after offload a `SortExec` is a
 * `SortExecTransformer`. The two are the same operator, so a test asking "is there a sort here"
 * should get the same answer either way.
 *
 * Only same-operator pairs belong here. A test that asserts `SortMergeJoinExec` where Gluten
 * deliberately plans a shuffled hash join is asserting something that is genuinely no longer true,
 * and still needs an exclusion or a rewrite.
 */
object GlutenVanillaPlanView {

  /**
   * The vanilla operator this plan node is the offloaded form of, if any.
   *
   * Children are left as they are: the returned node exists to be type-matched and to have its own
   * fields read, not to be executed or traversed.
   */
  def vanillaEquivalent(plan: SparkPlan): Option[SparkPlan] = plan match {
    case s: SortExecTransformer =>
      Some(SortExec(s.sortOrder, s.global, s.child, s.testSpillFrequency))
    case p: ProjectExecTransformerBase =>
      Some(ProjectExec(p.list, p.input))
    case f: FilterExecTransformerBase =>
      Some(FilterExec(f.cond, f.input))
    case w: WindowExecTransformer =>
      Some(WindowExec(w.windowExpression, w.partitionSpec, w.orderSpec, w.child))
    case _ => None
  }

  /**
   * Widens a predicate so it holds for an offloaded operator whenever it holds for the vanilla one.
   */
  def widenPredicate(equivalent: SparkPlan => Option[SparkPlan])(
      f: SparkPlan => Boolean): SparkPlan => Boolean =
    plan => f(plan) || equivalent(plan).exists(f)

  /**
   * Widens a partial function the same way. The original node is tried first, so a Gluten test that
   * matches on `SortExecTransformer` keeps working unchanged.
   */
  def widenPartial[B](equivalent: SparkPlan => Option[SparkPlan])(
      pf: PartialFunction[SparkPlan, B]): PartialFunction[SparkPlan, B] =
    new PartialFunction[SparkPlan, B] {
      override def isDefinedAt(plan: SparkPlan): Boolean =
        pf.isDefinedAt(plan) || equivalent(plan).exists(pf.isDefinedAt)

      override def apply(plan: SparkPlan): B =
        if (pf.isDefinedAt(plan)) pf(plan) else pf(equivalent(plan).get)
    }
}

/**
 * Makes the plan-walking helpers Spark's own tests use ([[AdaptiveSparkPlanHelper]]) match an
 * offloaded operator wherever they would have matched the vanilla one, so that
 * `collect(plan) { case _: SortExec => }` in an unmodified Spark test keeps working under Gluten.
 *
 * Mix in after [[AdaptiveSparkPlanHelper]] -- which mixing it into a Gluten test trait does, since
 * the Gluten trait comes last in the suite's linearization.
 *
 * This is additive: a predicate that already matches is applied to the node as it is, so Gluten's
 * own assertions on `*Transformer` types are unaffected. It also does not touch `plan.collect { }`
 * (the method on `TreeNode`), which tests sometimes use instead and which cannot be intercepted.
 */
trait GlutenVanillaPlanView extends AdaptiveSparkPlanHelper {
  import GlutenVanillaPlanView._

  /** Override to extend the mapping for one suite, or to narrow it. */
  protected def vanillaEquivalentOf(plan: SparkPlan): Option[SparkPlan] = vanillaEquivalent(plan)

  override def find(p: SparkPlan)(f: SparkPlan => Boolean): Option[SparkPlan] =
    super.find(p)(widenPredicate(vanillaEquivalentOf _)(f))

  override def collect[B](p: SparkPlan)(pf: PartialFunction[SparkPlan, B]): Seq[B] =
    super.collect(p)(widenPartial(vanillaEquivalentOf _)(pf))

  override def collectFirst[B](p: SparkPlan)(pf: PartialFunction[SparkPlan, B]): Option[B] =
    super.collectFirst(p)(widenPartial(vanillaEquivalentOf _)(pf))

  override def collectWithSubqueries[B](p: SparkPlan)(
      pf: PartialFunction[SparkPlan, B]): Seq[B] =
    super.collectWithSubqueries(p)(widenPartial(vanillaEquivalentOf _)(pf))
}
