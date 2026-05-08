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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ExpandExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

/**
 * Physical plan rule that pushes a partial [[HashAggregateExec]] through an [[ExpandExec]] so that
 * pre-aggregation happens on the original (un-expanded) rows.
 *
 * Actual Q67 physical plan produced by Spark:
 *
 * HashAggregateExec (final) Exchange (shuffle by [grouping_keys..., spark_grouping_id])
 * HashAggregateExec (partial) <-- sees 9x expanded rows ExpandExec (9 projections for ROLLUP)
 * Project BroadcastHashJoin ...
 *
 * After this rule:
 *
 * HashAggregateExec (final) Exchange HashAggregateExec (partial-merge) <-- merges per
 * (grouping_keys, gid) ExpandExec (augmented) <-- pass-through + null-fill HashAggregateExec
 * (partial) <-- pre-agg on original rows, no gid Project BroadcastHashJoin ...
 */
object PushPartialAggThroughExpand extends Rule[SparkPlan] with Logging {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.get.pushAggregateThroughExpandEnabled) {
      return plan
    }
    logInfo(
      s"PushPartialAggThroughExpand rule is enabled. Plan root: ${plan.getClass.getSimpleName}")

    // Skip AdaptiveSparkPlanExec - it will be optimized during AQE execution
    // This rule should run before AQE is inserted
    plan match {
      case _: AdaptiveSparkPlanExec =>
        logInfo("Skipping AdaptiveSparkPlanExec - rule should run before AQE")
        return plan
      case _ =>
    }

    val result = plan.transformUp {
      case agg: HashAggregateExec =>
        logInfo(s"Found HashAggregateExec: aggExprs=${agg.aggregateExpressions.size}, " +
          s"modes=${agg.aggregateExpressions.map(_.mode).mkString(",")}, " +
          s"child=${agg.child.getClass.getSimpleName}")

        if (
          agg.aggregateExpressions.nonEmpty &&
          agg.aggregateExpressions.forall(_.mode == Partial) &&
          agg.child.isInstanceOf[ExpandExec]
        ) {
          logInfo(s"Found partial HashAggregate with Expand child")
          val expand = agg.child.asInstanceOf[ExpandExec]
          if (isEligible(agg, expand)) {
            logInfo(s"Pushing partial aggregation through Expand")
            rewrite(agg, expand)
          } else {
            logInfo(s"Not eligible for optimization")
            agg
          }
        } else {
          agg
        }
      case other =>
        other
    }

    logInfo(s"PushPartialAggThroughExpand rule finished")
    result
  }

  // -------------------------------------------------------------------------
  // Eligibility
  // -------------------------------------------------------------------------

  private def isEligible(agg: HashAggregateExec, expand: ExpandExec): Boolean = {
    logInfo(s"Checking eligibility with ${agg.aggregateExpressions.size} aggregates")

    // 1. All aggregate functions must be DeclarativeAggregate (decomposable).
    //    TypedImperativeAggregate has an opaque buffer we cannot split.
    val allDeclarative = agg.aggregateExpressions.forall(
      _.aggregateFunction.isInstanceOf[DeclarativeAggregate])
    logInfo(s"Check 1 - All DeclarativeAggregate: $allDeclarative")
    if (!allDeclarative) {
      logInfo(s"Failed: Not all aggregates are DeclarativeAggregate")
      return false
    }

    val expandChildOutputSet = expand.child.outputSet
    val childOutputStr = expandChildOutputSet.map(a => s"${a.name}#${a.exprId}").mkString(", ")
    logInfo(s"Expand child output: $childOutputStr")

    // Build a mapping from Expand output attributes to child attributes by name
    // Expand creates new attributes but they correspond to child attributes by name
    val expandOutputToChildAttr = expand.output.flatMap {
      expandAttr => expandChildOutputSet.find(_.name == expandAttr.name).map(expandAttr -> _)
    }.toMap

    // 2. At least one grouping key must originate from Expand's child
    //    (i.e. something meaningful to pre-aggregate on before expansion).
    val hasPreExpandKey = agg.groupingExpressions.exists {
      e =>
        e.references.forall {
          ref =>
            expandOutputToChildAttr.get(ref).exists(
              childAttr =>
                expandChildOutputSet.contains(childAttr))
        }
    }
    logInfo(s"Check 2 - Has pre-expand grouping key: $hasPreExpandKey")
    val groupingStr = agg.groupingExpressions.map {
      e =>
        val refs = e.references.map(r => s"${r.name}#${r.exprId}").mkString(",")
        s"${e.sql} [refs: $refs]"
    }.mkString("; ")
    logInfo(s"Grouping expressions: $groupingStr")
    if (!hasPreExpandKey) {
      logInfo(s"Failed: No grouping key from before Expand")
      return false
    }

    // 3. The inputs to the aggregate functions (the measure expressions) must
    //    all come from Expand's child - Expand must pass them through unchanged.
    //    In Q67: ss_sales_price * ss_quantity are in every Expand projection
    //    unchanged, so this passes.  If a measure referenced a null-filled
    //    column it would be wrong to pre-aggregate.
    val expandInjectedAttrs = expand.output.toSet -- expandChildOutputSet
    logInfo(s"Expand injected attrs: ${expandInjectedAttrs.map(_.name).mkString(", ")}")
    val measureRefs = agg.aggregateExpressions.flatMap(
      _.aggregateFunction.children.flatMap(_.references)
    )
    logInfo(s"Measure references: ${measureRefs.map(_.name).mkString(", ")}")
    val measureUsesInjected = measureRefs.exists(expandInjectedAttrs.contains)
    logInfo(s"Check 3 - Measures use injected attrs: $measureUsesInjected")
    if (measureUsesInjected) {
      logInfo(s"Failed: Measure expressions reference Expand-injected attributes")
      return false
    }

    // 4. No DISTINCT aggregates - distinct requires all raw values.
    val hasDistinct = agg.aggregateExpressions.exists(_.isDistinct)
    logInfo(s"Check 4 - Has DISTINCT: $hasDistinct")
    if (hasDistinct) {
      logInfo(s"Failed: Contains DISTINCT aggregates")
      return false
    }

    logInfo(s"All eligibility checks passed!")
    true
  }

  // -------------------------------------------------------------------------
  // Rewrite
  // -------------------------------------------------------------------------

  private def rewrite(
      partialAgg: HashAggregateExec,
      expand: ExpandExec
  ): SparkPlan = {
    val expandChild = expand.child
    val expandChildOutputSet = expandChild.outputSet

    // Map from original child attributes to their names for lookup
    val childAttrByName = expandChildOutputSet.map(a => a.name -> a).toMap

    // Identify which attributes are used in grouping (dimensions) vs aggregation (measures)
    val groupingAttrNames = partialAgg.groupingExpressions.flatMap(_.references).map(_.name).toSet
    val measureAttrNames = partialAgg.aggregateExpressions
      .flatMap(_.aggregateFunction.children.flatMap(_.references))
      .map(_.name).toSet

    // Dimension attributes from child (used in grouping, not in measures)
    val dimensionAttrs = expandChildOutputSet.filter(
      a =>
        groupingAttrNames.contains(a.name) && !measureAttrNames.contains(a.name))

    logInfo(s"Dimension attrs: ${dimensionAttrs.map(_.name).mkString(", ")}")
    logInfo(s"Measure attr names: ${measureAttrNames.mkString(", ")}")

    // ---- Step 1: lower partial agg (new, below Expand) --------------------
    // Groups on dimension columns only, aggregates the measures

    val lowerGroupingExprs = dimensionAttrs.toSeq.map(a => a: NamedExpression)

    val lowerAggExprs: Seq[AggregateExpression] =
      partialAgg.aggregateExpressions.map {
        ae => ae.copy(mode = Partial, resultId = NamedExpression.newExprId)
      }

    val lowerDeclarativeAggs: Seq[DeclarativeAggregate] =
      lowerAggExprs.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])

    // Buffer attributes produced by the lower agg
    val lowerBufferAttrs: Seq[Attribute] =
      lowerDeclarativeAggs.flatMap(_.aggBufferAttributes)

    // Result: dimension attrs + buffer attrs
    val lowerResultExprs: Seq[NamedExpression] =
      lowerGroupingExprs.map(_.toAttribute) ++ lowerBufferAttrs

    val lowerAgg = HashAggregateExec(
      requiredChildDistributionExpressions = None,
      isStreaming = false,
      numShufflePartitions = None,
      groupingExpressions = lowerGroupingExprs,
      aggregateExpressions = lowerAggExprs,
      aggregateAttributes = lowerAggExprs.map(_.resultAttribute),
      initialInputBufferOffset = 0,
      resultExpressions = lowerResultExprs,
      child = expandChild
    )

    // Build mapping from child attr names to lower agg output attrs
    val lowerAggOutputByName = lowerAgg.output.map(a => a.name -> a).toMap

    // ---- Step 2: augmented ExpandExec (on top of lowerAgg) ----------------
    // Rewrite projections to reference lower agg output instead of original child

    val augmentedProjections: Seq[Seq[Expression]] =
      expand.projections.map {
        proj =>
          val rewrittenProj = proj.flatMap {
            case a: Attribute if measureAttrNames.contains(a.name) =>
              // Measure columns are now aggregated into buffers - skip them
              None
            case a: Attribute if lowerAggOutputByName.contains(a.name) =>
              // Map to corresponding attribute from lower agg output
              Some(lowerAggOutputByName(a.name))
            case Literal(null, dt) =>
              // Null literals for null-padded columns - keep as is
              Some(Literal(null, dt))
            case other =>
              // Other expressions (like gid literal) - keep as is
              Some(other)
          }
          rewrittenProj ++ lowerBufferAttrs // Append buffer attrs to pass through
      }

    // Build new output schema: only dimension attrs (not measures) + buffer attrs
    val augmentedOutput: Seq[Attribute] = expand.output.filterNot(
      a =>
        measureAttrNames.contains(a.name)) ++ lowerBufferAttrs

    val augmentedExpand = ExpandExec(augmentedProjections, augmentedOutput, lowerAgg)

    // ---- Step 3: upper merge agg (replaces the original partial agg) ------
    //
    // Grouping keys: same as original partialAgg (includes gid - which now
    // exists in the augmented Expand output).
    //
    // Agg mode: Partial -> PartialMerge.  In PartialMerge mode, Spark's
    // DeclarativeAggregate reads from inputAggBufferAttributes instead of
    // evaluating the original expressions against raw input rows.
    //
    // initialInputBufferOffset: number of positions in the input row before
    // the agg buffer starts.  The augmented Expand output is:
    //   [expand original output (grouping cols + gid), lowerBufferAttrs]
    //   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^   ^^^^^^^^^^^^^^^^
    //         = expand.output.size positions           buffer starts here
    val upperInputBufferOffset = expand.output.size

    val upperAggExprs: Seq[AggregateExpression] =
      partialAgg.aggregateExpressions.map(_.copy(mode = PartialMerge))

    val upperAgg = partialAgg.copy(
      aggregateExpressions = upperAggExprs,
      aggregateAttributes = upperAggExprs.map(_.resultAttribute),
      initialInputBufferOffset = upperInputBufferOffset,
      child = augmentedExpand
    )

    upperAgg
  }
}
