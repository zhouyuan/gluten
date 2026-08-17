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

import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.execution._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.EXCHANGE
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType}

/**
 * To transform regular aggregation to intermediate aggregation that internally enables
 * optimizations such as flushing and abandoning.
 */
case class FlushableHashAggregateRule(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!VeloxConfig.get.enableVeloxFlushablePartialAggregation) {
      return plan
    }
    plan.transformUpWithPruning(_.containsPattern(EXCHANGE)) {
      case s: ShuffleExchangeLike =>
        // If an exchange follows a hash aggregate in which all functions are in partial mode,
        // then it's safe to convert the hash aggregate to flushable hash aggregate.
        val out = s.withNewChildren(
          List(replaceEligibleAggregates(s.child))
        )
        out
    }
  }

  private def aggregatesNotSupportFlush(aggExprs: Seq[AggregateExpression]): Boolean = {
    if (VeloxConfig.get.floatingPointMode == "loose") {
      return false
    }

    def isFloatingPointType(dataType: DataType): Boolean = {
      dataType == DoubleType || dataType == FloatType
    }

    def isUnsupportedAggregation(aggExpr: AggregateExpression): Boolean = {
      aggExpr.aggregateFunction match {
        case Sum(child, _) if isFloatingPointType(child.dataType) => true
        case Average(child, _) if isFloatingPointType(child.dataType) => true
        case _ => false
      }
    }

    aggExprs.exists(isUnsupportedAggregation)
  }

  /**
   * Walks the exchange-free region below an exchange downward, converting every
   * RegularHashAggregateExecTransformer / SortHashAggregateExecTransformer that is eligible into
   * its flushable variant. An aggregate is eligible when all expressions are Partial/PartialMerge,
   * it is not the protected PartialMerge aggregate directly below a distinct-partial aggregate, and
   * no aggregate function disallows flushing.
   *
   * The walk always continues below the current node, whether or not that node was converted. A
   * single exchange-free region can hold several intermediate aggregates stacked on top of each
   * other - for instance the pre-shuffle PartialMerge stage that `ImplementJoinAggregate` plans
   * above a join together with the Partial aggregate it pushed below that same join - and an
   * aggregate that cannot be converted must not hide the ones underneath it.
   */
  private def replaceEligibleAggregates(plan: SparkPlan): SparkPlan = {
    def toFlushableAgg(agg: HashAggregateExecTransformer): FlushableHashAggregateExecTransformer = {
      FlushableHashAggregateExecTransformer(
        agg.requiredChildDistributionExpressions,
        agg.groupingExpressions,
        agg.aggregateExpressions,
        agg.aggregateAttributes,
        agg.initialInputBufferOffset,
        agg.resultExpressions,
        agg.child
      )
    }

    /**
     * `parentIsDistinctPartialAgg` marks the aggregate that must stay regular in Spark's
     * one-distinct aggregation pipeline:
     *
     * RegularHashAggregateExecTransformer [k] [count(distinct v)] // finalAggregate +-
     * RegularHashAggregateExecTransformer [k] [count(distinct v)] // partialDistinctAggregate +-
     * RegularHashAggregateExecTransformer [k, v] [count(...)] // partialMergeAggregate +-
     * ColumnarExchange hashpartitioning(k, v, 200) +- RegularHashAggregateExecTransformer [k, v]
     * [count(...)] // partialAggregate +- ...
     *
     * The `partialMergeAggregate` is the step that materializes the de-duplicated `(k, v)` stream
     * consumed by the distinct-partial aggregate above it. If it flushes, duplicate `(k, v)` keys
     * may be reintroduced within one partition and the distinct aggregation pipeline would no
     * longer see the shape Spark planned for.
     */
    def isEligible(
        agg: HashAggregateExecTransformer,
        parentIsDistinctPartialAgg: Boolean): Boolean =
      if (!agg.aggregateExpressions.forall(p => p.mode == Partial || p.mode == PartialMerge)) {
        // Not an intermediate agg.
        false
      } else if (
        parentIsDistinctPartialAgg && agg.aggregateExpressions.forall(_.mode == PartialMerge)
      ) {
        // Protected: see the note above.
        false
      } else if (aggregatesNotSupportFlush(agg.aggregateExpressions)) {
        // Aggregate uses a function that is unsafe to flush.
        false
      } else {
        true
      }

    def isDistinctPartialAgg(plan: SparkPlan): Boolean = plan match {
      case agg: HashAggregateExecTransformer =>
        agg.aggregateExpressions.exists(expr => expr.isDistinct && expr.mode == Partial)
      case _ => false
    }

    def transformDown(plan: SparkPlan, parentIsDistinctPartialAgg: Boolean): SparkPlan =
      plan match {
        case exchange: ShuffleExchangeLike =>
          // Stop at the next exchange. This rule is applied from an exchange boundary and should
          // not continue rewriting into a different shuffle region.
          exchange
        case other =>
          // Decide eligibility on the node as it stands now: rebuilding the children below hands
          // out a fresh plan node, so any identity-based bookkeeping has to happen first.
          val eligible = other match {
            case agg: RegularHashAggregateExecTransformer =>
              isEligible(agg, parentIsDistinctPartialAgg)
            case agg: SortHashAggregateExecTransformer =>
              isEligible(agg, parentIsDistinctPartialAgg)
            case _ => false
          }
          val isDistinctPartial = isDistinctPartialAgg(other)
          val rewritten =
            other.withNewChildren(other.children.map(transformDown(_, isDistinctPartial)))
          if (eligible) {
            // `withNewChildren` keeps the node type, so the cast below is safe.
            toFlushableAgg(rewritten.asInstanceOf[HashAggregateExecTransformer])
          } else {
            rewritten
          }
      }

    transformDown(plan, parentIsDistinctPartialAgg = false)
  }
}
