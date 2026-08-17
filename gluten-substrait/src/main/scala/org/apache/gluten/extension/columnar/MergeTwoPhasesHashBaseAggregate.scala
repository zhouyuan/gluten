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
package org.apache.gluten.extension.columnar

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}

/**
 * Merge two phase hash-based aggregate into one aggregate in the spark plan if there is no shuffle:
 *
 * Merge HashAggregate(t1.i, SUM, final) + HashAggregate(t1.i, SUM, partial) into
 * HashAggregate(t1.i, SUM, complete)
 *
 * Likewise HashAggregate(t1.i, SUM, final) + HashAggregate(t1.i, SUM, partialMerge) collapses into
 * HashAggregate(t1.i, SUM, final), because without a shuffle in between the local merge only
 * duplicates work the final merge does anyway.
 *
 * Note: this rule must be applied before the `PullOutPreProject` rule, because the
 * `PullOutPreProject` rule will modify the attributes in some cases.
 */
case class MergeTwoPhasesHashBaseAggregate(session: SparkSession)
  extends Rule[SparkPlan]
  with Logging {

  val glutenConf: GlutenConfig = GlutenConfig.get
  val scanOnly: Boolean = glutenConf.enableScanOnly
  val enableColumnarHashAgg: Boolean = !scanOnly && glutenConf.enableColumnarHashAgg
  val replaceSortAggWithHashAgg: Boolean = GlutenConfig.get.forceToUseHashAgg
  val mergeTwoPhasesAggEnabled: Boolean = GlutenConfig.get.mergeTwoPhasesAggEnabled

  /**
   * A PartialMerge aggregate sitting directly below a Final aggregate means `EnsureRequirements`
   * found no shuffle necessary between them: the child was already clustered on the grouping keys.
   * The local merge then only pre-does work the Final aggregate performs anyway, so it can be
   * dropped and the Final aggregate can read the aggregate buffers straight from the child.
   *
   * This shape is produced by `ImplementJoinAggregate`, which always splits the aggregate above a
   * pushed-through-join aggregate into PartialMerge + Final because it cannot know at planning time
   * whether the join output will already be clustered on the grouping keys.
   */
  private def isRedundantPartialMergeAgg(
      partialMergeAgg: BaseAggregateExec,
      finalAgg: BaseAggregateExec): Boolean = {
    if (
      !partialMergeAgg.aggregateExpressions.forall(_.mode == PartialMerge) ||
      !finalAgg.aggregateExpressions.forall(_.mode == Final) ||
      partialMergeAgg.aggregateExpressions.size != finalAgg.aggregateExpressions.size
    ) {
      return false
    }
    // Both stages must merge the very same buffers, positionally, and must come from one logical
    // aggregate. Otherwise they are two independent aggregations that happen to be adjacent.
    val sameFunctions = partialMergeAgg.aggregateExpressions.map(_.aggregateFunction) ==
      finalAgg.aggregateExpressions.map(_.aggregateFunction)
    // Only plain grouping attributes: for a derived grouping expression the dropped stage is what
    // evaluates it, and the final merge's required child distribution still refers to the
    // attribute that stage produced.
    val sameGrouping = partialMergeAgg.groupingExpressions.forall(_.isInstanceOf[Attribute]) &&
      finalAgg.groupingExpressions == partialMergeAgg.groupingExpressions.map(_.toAttribute)
    val sameLogicalLink = (finalAgg.logicalLink, partialMergeAgg.logicalLink) match {
      case (Some(agg1), Some(agg2)) => agg1.sameResult(agg2)
      case _ => false
    }
    sameFunctions && sameGrouping && sameLogicalLink
  }

  private def isPartialAgg(partialAgg: BaseAggregateExec, finalAgg: BaseAggregateExec): Boolean = {
    // Aggregates with a FILTER clause can be merged as long as the FILTER predicate is carried
    // over to the Complete mode aggregate. Note the physical final aggregate has its FILTER
    // stripped (Spark's AggUtils.mayRemoveAggFilters only keeps FILTER in Partial/Complete modes),
    // so the FILTER must be restored from the partial aggregate when merging. Spark's aggregate
    // planning produces the partial and final aggregate expression lists together and keeps them
    // positionally aligned (the final phase reads the partial buffer by position), so a
    // partial/final pair can be matched by position. We cannot match by `resultId`: for a single
    // distinct aggregate, `AggUtils.planAggregateWithOneDistinct` builds the partial and final
    // distinct expressions with fresh `AggregateExpression` instances, so their `resultId`s differ.
    if (
      partialAgg.aggregateExpressions.forall(x => x.mode == Partial) &&
      finalAgg.aggregateExpressions.forall(x => x.mode == Final) &&
      partialAgg.aggregateExpressions.size == finalAgg.aggregateExpressions.size
    ) {
      (finalAgg.logicalLink, partialAgg.logicalLink) match {
        case (Some(agg1), Some(agg2)) => agg1.sameResult(agg2)
        case _ => false
      }
    } else {
      false
    }
  }

  /**
   * Builds Complete mode aggregate expressions from the final aggregate. The physical final
   * aggregate no longer carries the FILTER predicate (see `isPartialAgg`), so the FILTER is
   * restored from the partial aggregate. A partial/final pair is matched by position: Spark's
   * aggregate planning emits the two expression lists together and keeps them positionally aligned
   * (the final phase reads the partial buffer by position), and `isPartialAgg` has already checked
   * that the two lists have the same size.
   */
  private def toCompleteAggregateExpressions(
      partialAgg: BaseAggregateExec,
      finalAggExpressions: Seq[AggregateExpression]): Seq[AggregateExpression] = {
    finalAggExpressions.zip(partialAgg.aggregateExpressions).map {
      case (finalExpr, partialExpr) =>
        finalExpr.copy(mode = Complete, filter = partialExpr.filter)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!mergeTwoPhasesAggEnabled || !enableColumnarHashAgg) {
      plan
    } else {
      plan.transformDown {
        case hashAgg @ HashAggregateExec(
              _,
              isStreaming,
              _,
              _,
              aggregateExpressions,
              aggregateAttributes,
              _,
              resultExpressions,
              child: HashAggregateExec) if !isStreaming && isPartialAgg(child, hashAgg) =>
          // convert to complete mode aggregate expressions
          val completeAggregateExpressions =
            toCompleteAggregateExpressions(child, aggregateExpressions)
          hashAgg.copy(
            groupingExpressions = child.groupingExpressions,
            aggregateExpressions = completeAggregateExpressions,
            initialInputBufferOffset = 0,
            child = child.child
          )
        case hashAgg @ HashAggregateExec(
              _,
              isStreaming,
              _,
              _,
              _,
              _,
              _,
              _,
              child: HashAggregateExec)
            if !isStreaming && isRedundantPartialMergeAgg(child, hashAgg) =>
          // Drop the redundant local merge, keeping the final merge only. It now reads the
          // aggregate buffers from the same input the dropped stage read them from.
          hashAgg.copy(
            groupingExpressions = child.groupingExpressions,
            initialInputBufferOffset = child.initialInputBufferOffset,
            child = child.child
          )
        case objectHashAgg @ ObjectHashAggregateExec(
              _,
              isStreaming,
              _,
              _,
              aggregateExpressions,
              aggregateAttributes,
              _,
              resultExpressions,
              child: ObjectHashAggregateExec)
            if !isStreaming && isPartialAgg(child, objectHashAgg) =>
          // convert to complete mode aggregate expressions
          val completeAggregateExpressions =
            toCompleteAggregateExpressions(child, aggregateExpressions)
          objectHashAgg.copy(
            requiredChildDistributionExpressions = None,
            groupingExpressions = child.groupingExpressions,
            aggregateExpressions = completeAggregateExpressions,
            initialInputBufferOffset = 0,
            child = child.child
          )
        case sortAgg @ SortAggregateExec(
              _,
              isStreaming,
              _,
              _,
              aggregateExpressions,
              aggregateAttributes,
              _,
              resultExpressions,
              child: SortAggregateExec)
            if replaceSortAggWithHashAgg && !isStreaming && isPartialAgg(child, sortAgg) =>
          // convert to complete mode aggregate expressions
          val completeAggregateExpressions =
            toCompleteAggregateExpressions(child, aggregateExpressions)
          sortAgg.copy(
            requiredChildDistributionExpressions = None,
            groupingExpressions = child.groupingExpressions,
            aggregateExpressions = completeAggregateExpressions,
            initialInputBufferOffset = 0,
            child = child.child
          )
        case plan: SparkPlan => plan
      }
    }
  }
}
