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
    // We capture the full Alias nodes (not just exprId->child) to reconstruct output Attributes.
    val (expand, computedPreProjectAliases): (ExpandExecTransformer, Seq[Alias]) =
      agg.child match {
        case e: ExpandExecTransformer =>
          (e, Seq.empty)
        case project: ProjectExecTransformer if project.child.isInstanceOf[ExpandExecTransformer] =>
          val e = project.child.asInstanceOf[ExpandExecTransformer]
          // Collect only the aliases whose exprId is NOT already in the expand output;
          // pass-through columns reference the expand output directly.
          val expandOutputIds = e.output.map(_.exprId).toSet
          val computedAliases = project.projectList.collect {
            case alias: Alias if !expandOutputIds.contains(alias.exprId) => alias
          }
          (e, computedAliases)
        case _ => return None
      }

    val preProjectAliasMap: Map[ExprId, Expression] =
      computedPreProjectAliases.map(a => a.exprId -> a.child).toMap

    extractGroupingSets(expand, agg) match {
      case Some((groupingSets, _)) if isValidForMultiGroupingSet(agg, expand, groupingSets) =>
        val (resolvedExpand, groupingExprs, aggExprs) = if (computedPreProjectAliases.isEmpty) {
          // No pre-project: use the original expand and agg expressions unchanged.
          (expand, agg.groupingExpressions, agg.aggregateExpressions)
        } else {
          val aggRefsPreProject = agg.aggregateExpressions.exists { aggExpr =>
            aggExpr.aggregateFunction.children.exists {
              case attr: AttributeReference => preProjectAliasMap.contains(attr.exprId)
              case _ => false
            }
          }

          if (aggRefsPreProject) {
            // Aggregate function inputs reference computed pre-project aliases (e.g. SUM(b*c)
            // where b*c was computed by the pre-project). Velox requires aggregate inputs to be
            // field accesses (AggregateInfo.cpp:79), so we cannot simply inline these expressions
            // into the AggregateRel. Instead we fold the pre-project INTO the expand:
            //
            //   Before: Agg -> ProjectExecTransformer(expand_out -> computed_cols) -> Expand
            //   After:  Agg -> ExtendedExpand (projects = original ++ computed_cols per row)
            //
            // For each pre-project alias (expr over expand output attrs), we substitute each
            // expand output AttributeReference with the corresponding expression from that
            // projection row. Since computed aggregate inputs are never null-masked by expand
            // (only grouping keys get nulled), the substitution is identical across rows.
            // The extended expand output includes the new attributes (same exprIds as the
            // pre-project aliases), so the agg's aggregate expressions resolve to field accesses.
            // The ExpandRel remains the direct Substrait source of AggregateRel, so Velox
            // can fuse them into MultiGroupingSetHashAggregation.
            val expandOutputIdToPos: Map[ExprId, Int] =
              expand.output.zipWithIndex.map { case (a, i) => a.exprId -> i }.toMap

            def substituteInRow(expr: Expression, proj: Seq[Expression]): Expression =
              expr.transform {
                case attr: AttributeReference if expandOutputIdToPos.contains(attr.exprId) =>
                  proj(expandOutputIdToPos(attr.exprId))
              }

            val newProjections = expand.projections.map { proj =>
              proj ++ computedPreProjectAliases.map(a => substituteInRow(a.child, proj))
            }
            val newOutput = expand.output ++ computedPreProjectAliases.map(_.toAttribute)
            val newExpand = expand.copy(projections = newProjections, output = newOutput)

            // agg.groupingExpressions reference original expand output exprIds (present in
            // newOutput). agg.aggregateExpressions reference computed alias exprIds (also
            // present in newOutput). No inlining needed.
            (newExpand, agg.groupingExpressions, agg.aggregateExpressions)
          } else {
            // Aggregate function inputs are already simple references; only grouping
            // expressions may reference pre-project aliases. Inline those and drop the
            // pre-project so the expand remains the direct child.
            def inlineAliases(expr: Expression): Expression = expr.transform {
              case attr: AttributeReference if preProjectAliasMap.contains(attr.exprId) =>
                preProjectAliasMap(attr.exprId)
            }
            val g = agg.groupingExpressions.map(inlineAliases(_).asInstanceOf[NamedExpression])
            (expand, g, agg.aggregateExpressions)
          }
        }

        Some(
          MultiGroupingSetHashAggregateExecTransformer(
            agg.requiredChildDistributionExpressions,
            groupingExprs,
            aggExprs,
            agg.aggregateAttributes,
            agg.initialInputBufferOffset,
            agg.resultExpressions,
            // ExpandExecTransformer (possibly extended) as direct child. doTransform inherited
            // from HashAggregateExecTransformer emits ExpandRel -> AggregateRel, which Velox
            // fuses into MultiGroupingSetHashAggregation.
            resolvedExpand,
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
