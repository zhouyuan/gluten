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

/**
 * Converts the Spark ROLLUP/CUBE/GROUPING SETS plan pattern:
 *
 * {{{
 *   HashAggregateExecTransformer
 *     +-- [ProjectExecTransformer]   (optional; inserted by PullOutPreProject for SUM(b*c))
 *          +-- ExpandExecTransformer
 *               +-- rawChild
 * }}}
 *
 * into:
 *
 * {{{
 *   MultiGroupingSetHashAggregateExecTransformer(child = rawChild)
 * }}}
 *
 * The new node's [[doTransform]] constructs the Substrait ExpandRel + AggregateRel itself, with
 * projection rows guaranteed to have the grouping_id literal as the last column so that Velox's
 * LocalPlanner fusion check succeeds and creates a MultiGroupingSetHashAggregation operator
 * instead of the separate Expand + HashAggregation operators.
 *
 * Expression rewriting
 * --------------------
 * After the transformation, all expressions (groupingExpressions and aggregate function inputs)
 * must resolve against [[rawChild.output]].
 *
 *   - [[groupingExpressions]] from the original agg reference the expand output. The expand
 *     output passes grouping key columns through from rawChild, so we replace each reference with
 *     its corresponding rawChild attribute (via the expand's pass-through projection).
 *
 *   - Aggregate function inputs reference the expand output (or the optional pre-project output).
 *     We substitute them through the expand (and optional pre-project) back to rawChild attributes.
 *
 * If any expression cannot be traced back to rawChild (e.g. because it references a column that
 * is null-masked by the expand), we bail out and leave the plan unchanged.
 */
case class MultiGroupingSetAggregateRule(session: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!VeloxConfig.get.enableMultiGroupingSetHashAggregation) {
      return plan
    }
    plan.transformUp {
      case agg: RegularHashAggregateExecTransformer =>
        tryConvert(agg).getOrElse(agg)
      case agg: HashAggregateExecTransformer =>
        tryConvert(agg).getOrElse(agg)
    }
  }

  private def tryConvert(
      agg: HashAggregateExecTransformer): Option[MultiGroupingSetHashAggregateExecTransformer] = {

    // -----------------------------------------------------------------------
    // Step 1: Peel off the optional pre-project and find the ExpandExecTransformer.
    // -----------------------------------------------------------------------

    // expandOutputToExpr: maps expand output exprId -> expression in expand.child.output.
    // This lets us substitute agg expressions that reference expand output back to rawChild.
    val (expand, expandOutputToChildExpr): (ExpandExecTransformer, Map[ExprId, Expression]) =
      agg.child match {
        case e: ExpandExecTransformer =>
          // No pre-project. Expand output is a direct pass-through of its child's columns
          // (plus the grouping_id slot). Build the substitution map from the expand projections:
          // for each expand output attr, find the unique child expression (must be consistent
          // across all projection rows and never null-masked).
          val subMap = buildExpandPassThroughMap(e)
          (e, subMap)

        case proj: ProjectExecTransformer
            if proj.child.isInstanceOf[ExpandExecTransformer] =>
          val e = proj.child.asInstanceOf[ExpandExecTransformer]
          // Build the expand pass-through map for expand->rawChild substitution.
          val expandSubMap = buildExpandPassThroughMap(e)
          // Now also inline the pre-project aliases: pre-project output -> expand.child expr.
          // This gives us a map from pre-project output exprId -> rawChild expression.
          val expandOutputIds = e.output.map(_.exprId).toSet
          val projSubMap: Map[ExprId, Expression] = proj.projectList.flatMap {
            case alias: Alias if !expandOutputIds.contains(alias.exprId) =>
              // Computed alias: substitute its body through expandSubMap to get rawChild expr.
              val substituted = substituteExpression(alias.child, expandSubMap)
              substituted.map(alias.exprId -> _)
            case alias: Alias =>
              // Pass-through alias referencing expand output: look up in expandSubMap.
              alias.child match {
                case attr: AttributeReference =>
                  expandSubMap.get(attr.exprId).map(alias.exprId -> _)
                case _ => None
              }
            case attr: AttributeReference =>
              expandSubMap.get(attr.exprId).map(attr.exprId -> _)
            case _ => None
          }.toMap
          // Merge: pre-project exprIds take priority; fall back to expand map.
          (e, expandSubMap ++ projSubMap)

        case _ => return None
      }

    // -----------------------------------------------------------------------
    // Step 2: Extract grouping sets from the expand.
    // -----------------------------------------------------------------------
    val groupingSets = extractGroupingSets(expand, agg) match {
      case Some(gs) => gs
      case None => return None
    }

    if (groupingSets.size < 2) return None

    // -----------------------------------------------------------------------
    // Step 3: Validate aggregate modes.
    // -----------------------------------------------------------------------
    val validModes = agg.aggregateExpressions.forall(e => e.mode == Partial || e.mode == Complete)
    if (!validModes) return None

    // -----------------------------------------------------------------------
    // Step 4: Rewrite groupingExpressions to resolve against expand.child.output.
    // groupingExpressions reference expand output attrs. We substitute each
    // AttributeReference through expandOutputToChildExpr.
    // -----------------------------------------------------------------------
    val rawChild = expand.child
    val newGroupingExprs: Seq[NamedExpression] = agg.groupingExpressions.map { ne =>
      val substituted = substituteExpression(ne, expandOutputToChildExpr)
      substituted match {
        case Some(expr: NamedExpression) => expr
        case Some(expr) =>
          // Wrap in alias to preserve the original exprId/name.
          Alias(expr, ne.name)(ne.toAttribute.exprId, ne.toAttribute.qualifier)
        case None => return None // Cannot resolve; bail out.
      }
    }

    // -----------------------------------------------------------------------
    // Step 5: Rewrite aggregateExpressions to resolve against expand.child.output.
    // -----------------------------------------------------------------------
    val newAggExprs: Seq[AggregateExpression] = agg.aggregateExpressions.map { aggExpr =>
      val newAggFunc = aggExpr.aggregateFunction.mapChildren { child =>
        substituteExpression(child, expandOutputToChildExpr) match {
          case Some(expr) => expr
          case None => return None
        }
      }.asInstanceOf[AggregateFunction]
      aggExpr.copy(aggregateFunction = newAggFunc)
    }

    // -----------------------------------------------------------------------
    // Step 6: Create the new node with rawChild as direct child.
    // -----------------------------------------------------------------------
    Some(
      MultiGroupingSetHashAggregateExecTransformer(
        agg.requiredChildDistributionExpressions,
        newGroupingExprs,
        newAggExprs,
        agg.aggregateAttributes,
        agg.initialInputBufferOffset,
        agg.resultExpressions,
        rawChild,
        groupingSets
      ))
  }

  /**
   * Build a substitution map: expand output exprId -> expression in expand.child.output.
   * Only includes columns that are passed through consistently in ALL projection rows
   * (i.e., the same field reference in every row, never null-masked).
   * Columns that are null-masked in some rows (grouping keys that go absent) are NOT included.
   */
  private def buildExpandPassThroughMap(
      expand: ExpandExecTransformer): Map[ExprId, Expression] = {
    expand.output.zipWithIndex.flatMap {
      case (outAttr, pos) =>
        val rowExprs = expand.projections.map(_(pos))
        val fieldRefs = rowExprs.collect { case r: AttributeReference => r }
        // All rows must use the same field reference (never null-masked).
        if (
          fieldRefs.size == expand.projections.size &&
          fieldRefs.map(_.exprId).distinct.size == 1
        ) {
          Some(outAttr.exprId -> fieldRefs.head)
        } else {
          None // null-masked in at least one row (e.g. a grouping key)
        }
    }.toMap
  }

  /**
   * Substitute all AttributeReferences in `expr` using `subMap`. Returns None if any reference
   * cannot be resolved (i.e., it references a column absent from the map).
   */
  private def substituteExpression(
      expr: Expression,
      subMap: Map[ExprId, Expression]): Option[Expression] = {
    var ok = true
    val result = expr.transform {
      case attr: AttributeReference =>
        subMap.get(attr.exprId) match {
          case Some(replacement) => replacement
          case None =>
            // Check whether this attr is already in rawChild (not from expand).
            // If it's not in the map at all, we can't resolve it.
            ok = false
            attr
        }
    }
    if (ok) Some(result) else None
  }

  /**
   * Extract grouping sets from the ExpandExecTransformer.
   *
   * Each returned element is the list of grouping key indices (into agg.groupingExpressions) that
   * are ACTIVE (non-null) in that grouping set's projection row.
   */
  private def extractGroupingSets(
      expand: ExpandExecTransformer,
      agg: HashAggregateExecTransformer): Option[Seq[Seq[Int]]] = {
    if (expand.projections.isEmpty) return None

    // Map expand output exprId -> column position in projection rows.
    val expandOutputIdToPos: Map[ExprId, Int] =
      expand.output.zipWithIndex.map { case (attr, i) => attr.exprId -> i }.toMap

    // Find the column position for each grouping expression.
    val groupingPositions: Seq[Int] = agg.groupingExpressions.flatMap {
      case attr: AttributeReference => expandOutputIdToPos.get(attr.exprId)
      case alias: Alias =>
        alias.child match {
          case attr: AttributeReference => expandOutputIdToPos.get(attr.exprId)
          case _ => None
        }
      case _ => None
    }
    if (groupingPositions.size != agg.groupingExpressions.size) return None

    val groupingSets = expand.projections.map { projection =>
      groupingPositions.zipWithIndex.collect {
        case (pos, keyIdx)
            if pos < projection.size && !projection(pos).isInstanceOf[Literal] =>
          keyIdx
        case (pos, keyIdx)
            if pos < projection.size && projection(pos)
              .asInstanceOf[Literal]
              .value != null =>
          keyIdx
      }
    }

    Some(groupingSets)
  }
}

object MultiGroupingSetAggregateRule {
  // Helper methods can be added here if needed
}


import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.execution._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

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
            // Aggregate inputs reference computed pre-project aliases (e.g. SUM(b*c) where
            // b*c was computed by the pre-project referencing expand output attrs).
            //
            // Two Velox constraints prevent simple approaches:
            //   1. AggregateInfo.cpp:79 -- aggregate inputs must be field accesses/constants
            //   2. SubstraitToVeloxPlan.cc:924 -- expand projections must be fields or literals
            //
            // Solution: push the pre-project computation BELOW the expand so that:
            //   - A new ProjectExecTransformer under the expand pre-computes the expressions
            //     on the raw (unexpanded) data
            //   - The expand passes those columns through as plain field references (allowed)
            //   - The aggregate sees them as field accesses into the expand output (allowed)
            //   - ExpandRel remains the direct source of AggregateRel -- Velox fuses them
            //
            // Safety: the referenced expand output attrs must flow unchanged in EVERY expand
            // row (i.e., never null-masked). Grouping keys ARE null-masked in some rows, so
            // if a computed expression references a grouping key, the semantics would change
            // and we bail out. In practice, aggregate inputs reference data columns that are
            // never null-masked by the expand (only grouping key columns are nulled).

            // Map expand output exprId -> the unique child AttributeReference that feeds it,
            // but ONLY for positions that are never null-masked (same field ref in ALL rows).
            val expandOutToChildRef: Map[ExprId, AttributeReference] =
              expand.output.zipWithIndex.flatMap { case (outAttr, pos) =>
                val rowExprs = expand.projections.map(_(pos))
                val fieldRefs = rowExprs.collect { case r: AttributeReference => r }
                // All rows must yield a consistent field reference (never a null literal).
                if (
                  fieldRefs.size == expand.projections.size &&
                  fieldRefs.map(_.exprId).distinct.size == 1
                ) {
                  Some(outAttr.exprId -> fieldRefs.head)
                } else None
              }.toMap

            // Re-express each pre-project alias in terms of expand.child output.
            // Returns None if any referenced expand output attr is null-masked (bail out).
            def substituteWithChildRefs(expr: Expression): Option[Expression] = {
              var ok = true
              val result = expr.transform {
                case attr: AttributeReference =>
                  expandOutToChildRef.get(attr.exprId) match {
                    case Some(childRef) => childRef
                    case None => ok = false; attr
                  }
              }
              if (ok) Some(result) else None
            }

            val substitutedAliasOpts = computedPreProjectAliases.map { alias =>
              substituteWithChildRefs(alias.child).map { subExpr =>
                Alias(subExpr, alias.name)(alias.exprId, alias.qualifier, alias.explicitMetadata)
              }
            }
            if (substitutedAliasOpts.exists(_.isEmpty)) return None
            val substitutedAliases = substitutedAliasOpts.flatten

            // Create ProjectExecTransformer below the expand: pass through expand.child.output
            // plus the newly computed columns.
            val preExpandProjList: Seq[NamedExpression] =
              expand.child.output.map(_.asInstanceOf[NamedExpression]) ++ substitutedAliases
            val preExpandProject = ProjectExecTransformer(preExpandProjList, expand.child)

            // Extend every expand projection row with a plain AttributeReference to each
            // computed column. These are field accesses -- allowed by Velox ExpandNode.
            val computedAttrRefs: Seq[AttributeReference] = substitutedAliases.map { a =>
              AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
                a.exprId,
                a.qualifier)
            }

            // CRITICAL: Velox's LocalPlanner fusion check (LocalPlanner.cpp) requires that the
            // LAST element of every projection row is a ConstantTypedExpr (the grouping_id
            // literal). If we append computed refs at the end, row.back() becomes a field ref
            // and the fusion condition fails -- the ExpandNode executes normally, duplicating
            // rows N times. Fix: insert computed refs BEFORE the last entry (grouping_id) so
            // the last element stays the grouping_id literal constant in all rows.
            val newProjections = expand.projections.map { proj =>
              val (beforeGid, gidEntry) = proj.splitAt(proj.size - 1)
              beforeGid ++ computedAttrRefs ++ gidEntry
            }
            val newOutput = {
              val (beforeGid, gidAttr) = expand.output.splitAt(expand.output.size - 1)
              beforeGid ++ substitutedAliases.map(_.toAttribute) ++ gidAttr
            }
            val newExpand = expand.copy(
              projections = newProjections,
              output = newOutput,
              child = preExpandProject)

            // agg.groupingExpressions reference original expand output exprIds (in newOutput).
            // agg.aggregateExpressions reference computed alias exprIds (also in newOutput).
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
