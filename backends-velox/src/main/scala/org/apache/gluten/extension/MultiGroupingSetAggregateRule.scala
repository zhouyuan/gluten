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
    // Find the ExpandExecTransformer, possibly through a pre-project inserted by
    // PullOutPreProject (which adds a ProjectExec between the aggregate and expand when
    // aggregate expressions contain non-attribute children such as SUM(b + c)).
    val (expand, preProjectAliases): (ExpandExecTransformer, Map[ExprId, Expression]) =
      agg.child match {
        case e: ExpandExecTransformer =>
          (e, Map.empty)
        case project: ProjectExecTransformer if project.child.isInstanceOf[ExpandExecTransformer] =>
          val e = project.child.asInstanceOf[ExpandExecTransformer]
          // Build a substitution map for the extra attributes computed by the pre-project.
          // Pass-through columns (whose exprId already exists in the expand output) are not
          // included; their references remain valid against the expand output as-is.
          val expandOutputIds = e.output.map(_.exprId).toSet
          val aliases: Map[ExprId, Expression] = project.projectList.collect {
            case alias: Alias if !expandOutputIds.contains(alias.exprId) =>
              alias.exprId -> alias.child
          }.toMap
          (e, aliases)
        case _ => return None
      }

    extractGroupingSets(expand, agg) match {
      case Some((groupingSets, _)) if isValidForMultiGroupingSet(agg, expand, groupingSets) =>
        // If a pre-project was present, inline its computed aliases back into the aggregate's
        // grouping expressions so we can use the ExpandExecTransformer as the direct child.
        // However, do NOT inline aliases into aggregate function inputs: inlining would
        // produce complex expressions (e.g. coalesce(multiply(...), 0)) as aggregate inputs,
        // which Velox rejects with "Expression must be field access, constant, or lambda"
        // (AggregateInfo.cpp:79). When that case arises, fall back to regular processing
        // so the pre-project stays in the plan as an intermediate ProjectRel node.
        val (groupingExprs, aggExprs) = if (preProjectAliases.isEmpty) {
          (agg.groupingExpressions, agg.aggregateExpressions)
        } else {
          // Check whether any aggregate function input references a pre-project alias.
          val aggRefsPreProject = agg.aggregateExpressions.exists { aggExpr =>
            aggExpr.aggregateFunction.children.exists {
              case attr: AttributeReference => preProjectAliases.contains(attr.exprId)
              case _ => false
            }
          }
          if (aggRefsPreProject) return None

          // Only grouping expressions may reference pre-project aliases (safe to inline
          // since ROLLUP/CUBE grouping keys are always simple column refs after inlining).
          def inlineAliases(expr: Expression): Expression = expr.transform {
            case attr: AttributeReference if preProjectAliases.contains(attr.exprId) =>
              preProjectAliases(attr.exprId)
          }
          val g = agg.groupingExpressions.map(inlineAliases(_).asInstanceOf[NamedExpression])
          (g, agg.aggregateExpressions)
        }

        Some(
          MultiGroupingSetHashAggregateExecTransformer(
            agg.requiredChildDistributionExpressions,
            groupingExprs,
            aggExprs,
            agg.aggregateAttributes,
            agg.initialInputBufferOffset,
            agg.resultExpressions,
            // Use the ExpandExecTransformer directly as the child. doTransform (inherited from
            // HashAggregateExecTransformer) will emit ExpandRel → AggregateRel in the Substrait
            // plan, which Velox fuses into MultiGroupingSetHashAggregation.
            expand,
            groupingSets
          ))
      case _ => None
    }
  }

  /**
   * Extract grouping sets from the Expand operator. Returns the grouping sets and the actual child
   * (below the Expand).
   *
   * Each element of the returned grouping sets is a list of indices into
   * `agg.groupingExpressions` that are active (non-null) for that grouping set row.
   */
  private def extractGroupingSets(
      expand: ExpandExecTransformer,
      agg: HashAggregateExecTransformer): Option[(Seq[Seq[Int]], SparkPlan)] = {
    if (expand.projections.isEmpty) {
      return None
    }

    // Map each expand output attribute exprId to its column index in the projection rows.
    val expandOutputIdToPos: Map[ExprId, Int] =
      expand.output.zipWithIndex.map { case (attr, idx) => attr.exprId -> idx }.toMap

    // Find the projection-row position of each grouping expression.
    // groupingExpressions are AttributeReferences to expand output (either directly or
    // via the pre-project passthrough). Use exprId to locate the correct column.
    val groupingPositions: Seq[Int] = agg.groupingExpressions.flatMap {
      case attr: AttributeReference =>
        expandOutputIdToPos.get(attr.exprId)
      case alias: Alias =>
        alias.child match {
          case attr: AttributeReference => expandOutputIdToPos.get(attr.exprId)
          case _ => None
        }
      case _ => None
    }

    // All grouping expressions must be locatable in the expand output.
    if (groupingPositions.size != agg.groupingExpressions.size) {
      return None
    }

    // For each projection row (= one grouping set), record which grouping key indices are active.
    val groupingSets = expand.projections.map {
      projection =>
        groupingPositions.zipWithIndex.collect {
          case (pos, keyIdx) if pos < projection.size =>
            projection(pos) match {
              case Literal(null, _) => None // null-masked: not active in this grouping set
              case _ => Some(keyIdx) // active
            }
        }.flatten
    }

    Some((groupingSets, expand.child))
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
