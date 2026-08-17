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

import org.apache.gluten.execution.{FlushableHashAggregateExecTransformer, RegularHashAggregateExecTransformer}

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateMode, Final, Partial, PartialMerge, Sum}
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Plan-shape tests for [[FlushableHashAggregateRule]]. These only exercise the rule's traversal, so
 * they need no native library.
 */
class FlushableHashAggregateRuleSuite extends SharedSparkSession {
  private def leaf: SparkPlan = spark.range(0, 10).selectExpr("id AS v").queryExecution.sparkPlan

  private def agg(
      mode: AggregateMode,
      child: SparkPlan,
      isDistinct: Boolean = false): RegularHashAggregateExecTransformer = {
    val sum = AggregateExpression(Sum(child.output.head), mode, isDistinct)
    RegularHashAggregateExecTransformer(
      requiredChildDistributionExpressions = None,
      groupingExpressions = Seq.empty[NamedExpression],
      aggregateExpressions = Seq(sum),
      aggregateAttributes = Seq(sum.resultAttribute),
      initialInputBufferOffset = 0,
      resultExpressions = Seq(sum.resultAttribute),
      child = child
    )
  }

  /** Aggregate modes paired with whether that aggregate came out flushable, top down. */
  private def flushableByMode(plan: SparkPlan): Seq[(AggregateMode, Boolean)] = {
    FlushableHashAggregateRule(spark).apply(plan).collect {
      case a: FlushableHashAggregateExecTransformer =>
        (a.aggregateExpressions.head.mode, true)
      case a: RegularHashAggregateExecTransformer =>
        (a.aggregateExpressions.head.mode, false)
    }
  }

  test("stacked intermediate aggregates in one region all become flushable") {
    // The shape ImplementJoinAggregate plans: a pre-shuffle PartialMerge stage above the join with
    // the Partial aggregate that was pushed below that same join. Both sit in one exchange-free
    // region, so both must be converted - the rule must not stop at the upper one.
    val pushed = agg(Partial, leaf)
    val preShuffleMerge = agg(PartialMerge, ProjectExec(pushed.output, pushed))
    val plan = ShuffleExchangeExec(SinglePartition, preShuffleMerge)

    assert(flushableByMode(plan) == Seq((PartialMerge, true), (Partial, true)))
  }

  test("a non-intermediate aggregate does not hide the intermediate ones below it") {
    val partial = agg(Partial, leaf)
    val nonIntermediate = agg(Final, ProjectExec(partial.output, partial))
    val plan = ShuffleExchangeExec(SinglePartition, nonIntermediate)

    assert(flushableByMode(plan) == Seq((Final, false), (Partial, true)))
  }

  test("the PartialMerge stage feeding a distinct-partial aggregate stays regular") {
    // Spark's one-distinct pipeline. The PartialMerge stage materializes the de-duplicated stream
    // that the distinct-partial aggregate above it consumes, so it must not flush.
    val partialMerge = agg(PartialMerge, leaf)
    val distinctPartial = agg(Partial, partialMerge, isDistinct = true)
    val plan = ShuffleExchangeExec(SinglePartition, distinctPartial)

    assert(flushableByMode(plan) == Seq((Partial, true), (PartialMerge, false)))
  }

  test("aggregates below a nested exchange are left to their own region") {
    val inner = agg(Partial, leaf)
    val innerExchange = ShuffleExchangeExec(SinglePartition, inner)
    val outer = agg(PartialMerge, innerExchange)
    val plan = ShuffleExchangeExec(SinglePartition, outer)

    // `inner` still gets converted, but by the walk that starts at `innerExchange`, not by the one
    // that starts at the outer exchange.
    assert(flushableByMode(plan) == Seq((PartialMerge, true), (Partial, true)))
  }
}
