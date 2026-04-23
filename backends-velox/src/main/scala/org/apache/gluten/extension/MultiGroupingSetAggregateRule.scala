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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable

/**
 * Optimization rule to convert Expand-Aggregate pattern (used for GROUPING SETS/ROLLUP/CUBE) into
 * Velox's MultiGroupingSetHashAggregation which uses a shared hash table for better performance and
 * memory efficiency.
 *
 * The standard Spark approach for GROUPING SETS/ROLLUP/CUBE is:
 *   1. Expand: Duplicate rows for each grouping set, adding a grouping ID column 2. Aggregate:
 *      Perform aggregation on the expanded data
 *
 * This rule detects this pattern and replaces it with a single MultiGroupingSetHashAggregation that
 * processes all grouping sets in one pass with a shared hash table.
 */
case class MultiGroupingSetAggregateRule(session: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!VeloxConfig.get.enableMultiGroupingSetHashAggregation) {
      return plan
    }

    plan.transformUp {
      case agg: RegularHashAggregateExecTransformer =>
        tryConvertToMultiGroupingSetAgg(agg).getOrElse(agg)
      case agg: HashAggregateExecTransformer =>
        tryConvertToMultiGroupingSetAgg(agg).getOrElse(agg)
    }
  }

  /** Try to convert a hash aggregate with an expand child into a multi-grouping-set aggregate. */
  private def tryConvertToMultiGroupingSetAgg(
      agg: HashAggregateExecTransformer): Option[MultiGroupingSetHashAggregateExecTransformer] = {
    agg.child match {
      case expand: ExpandExecTransformer =>
        extractGroupingSets(expand, agg) match {
          case Some((groupingSets, _)) =>
            // Verify this is a valid pattern for multi-grouping-set optimization
            if (isValidForMultiGroupingSet(agg, expand, groupingSets)) {
              Some(
                MultiGroupingSetHashAggregateExecTransformer(
                  agg.requiredChildDistributionExpressions,
                  agg.groupingExpressions,
                  agg.aggregateExpressions,
                  agg.aggregateAttributes,
                  agg.initialInputBufferOffset,
                  agg.resultExpressions,
                  // Keep the ExpandExecTransformer as the child so doTransform produces
                  // ExpandRel → AggregateRel in the Substrait plan. Velox's
                  // SubstraitToVeloxPlan converts this to ExpandNode → AggregationNode,
                  // which the execution engine fuses into MultiGroupingSetHashAggregation.
                  expand,
                  groupingSets
                ))
            } else {
              None
            }
          case None => None
        }
      case _ => None
    }
  }

  /**
   * Extract grouping sets from the Expand operator. Returns the grouping sets and the actual child
   * (below the Expand).
   */
  private def extractGroupingSets(
      expand: ExpandExecTransformer,
      agg: HashAggregateExecTransformer): Option[(Seq[Seq[Int]], SparkPlan)] = {
    if (expand.projections.isEmpty) {
      return None
    }

    try {
      // Each projection in Expand represents one grouping set
      // We need to identify which grouping expressions are active in each set
      val groupingSets = mutable.ArrayBuffer[Seq[Int]]()
      val numGroupingExprs = agg.groupingExpressions.size

      // The last column in expand output is typically the grouping ID
      // We need to analyze the projections to determine which columns are nulled out
      for (projection <- expand.projections) {
        val activeGroupingIndices = mutable.ArrayBuffer[Int]()

        // Analyze each grouping expression position
        for (i <- 0 until numGroupingExprs) {
          if (i < projection.size) {
            projection(i) match {
              case Literal(null, _) =>
              // This grouping expression is not active in this set
              case _ =>
                // This grouping expression is active
                activeGroupingIndices += i
            }
          }
        }

        groupingSets += activeGroupingIndices.toSeq
      }

      Some((groupingSets.toSeq, expand.child))
    } catch {
      case _: Exception => None
    }
  }

  /** Check if the aggregate-expand pattern is valid for multi-grouping-set optimization. */
  private def isValidForMultiGroupingSet(
      agg: HashAggregateExecTransformer,
      expand: ExpandExecTransformer,
      groupingSets: Seq[Seq[Int]]): Boolean = {
    // Must have at least 2 grouping sets to benefit from shared hash table
    if (groupingSets.size < 2) {
      return false
    }

    // All aggregate expressions should be in Partial or Complete mode
    // (not PartialMerge or Final, as those would be in a different stage)
    val validModes = agg.aggregateExpressions.forall {
      aggExpr => aggExpr.mode == Partial || aggExpr.mode == Complete
    }

    if (!validModes) {
      return false
    }

    // Verify that the expand output matches what we expect
    // (grouping expressions + aggregate inputs + grouping ID)
    val expectedMinOutputSize = agg.groupingExpressions.size + 1 // +1 for grouping ID
    if (expand.output.size < expectedMinOutputSize) {
      return false
    }

    true
  }
}

object MultiGroupingSetAggregateRule {
  // Helper methods can be added here if needed
}

// Made with Bob
