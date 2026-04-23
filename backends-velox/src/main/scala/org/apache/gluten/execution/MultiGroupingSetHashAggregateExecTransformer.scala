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
package org.apache.gluten.execution

import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.substrait.{AggregationParams, SubstraitContext}
import org.apache.gluten.substrait.expression.ExpressionBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.IntegerType

import java.util.{ArrayList => JArrayList}

/**
 * Hash aggregation transformer for ROLLUP/CUBE/GROUPING SETS that maps directly to Velox's
 * MultiGroupingSetHashAggregation fused operator.
 *
 * Architecture
 * ------------
 * Spark represents ROLLUP/CUBE as: ExpandExec -> HashAggregateExec. The ExpandExec physically
 * duplicates each input row N times (once per grouping set), which is expensive.
 *
 * Velox has a fused operator MultiGroupingSetHashAggregation that processes all grouping sets in a
 * single pass over the input data using one shared hash table. It is triggered by LocalPlanner when
 * it sees AggregationNode whose direct source is an ExpandNode where every projection row has a
 * ConstantTypedExpr as its last element (the grouping_id literal).
 *
 * This node's [[doTransform]] constructs the Substrait plan directly:
 *   1. ExpandRel: projection rows are built from [[groupingSets]] metadata. Each row contains:
 *      - One entry per grouping key: field-ref if that key is active in this set, null literal
 *        otherwise.
 *      - Pass-through entries for any non-key columns needed by aggregate functions.
 *      - A grouping_id integer literal as the LAST entry (required by Velox's fusion check).
 *   2. AggregateRel on top of the ExpandRel.
 *
 * The [[child]] is the raw data producer (the input BELOW the original ExpandExec, not the
 * ExpandExecTransformer itself). All expressions ([[groupingExpressions]], aggregate function
 * children) must resolve against [[child.output]].
 *
 * @param groupingKeyExprs
 *   The grouping key expressions, each resolving against [[child.output]]. The position of each
 *   expression in this sequence corresponds to the indices in [[groupingSets]].
 * @param groupingSets
 *   The grouping sets. Each element is a sorted list of indices into [[groupingKeyExprs]] that are
 *   ACTIVE (non-null) for that grouping set. Used to build the ExpandRel projection rows.
 */
case class MultiGroupingSetHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan, // raw data source, NOT an ExpandExecTransformer
    groupingSets: Seq[Seq[Int]] // indices into groupingExpressions; active keys per grouping set
) extends HashAggregateExecTransformer(
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      child) {

  override protected def allowFlush: Boolean = false

  override def simpleString(maxFields: Int): String =
    s"MultiGroupingSet${super.simpleString(maxFields)}"

  override def verboseString(maxFields: Int): String =
    s"MultiGroupingSet${super.verboseString(maxFields)} groupingSets=$groupingSets"

  override protected def withNewChildInternal(newChild: SparkPlan): HashAggregateExecTransformer =
    copy(child = newChild)

  /**
   * Build the Substrait ExpandRel + AggregateRel directly.
   *
   * ExpandRel column layout (per projection row):
   *   [key_0, key_1, ..., key_{K-1}, passthrough_col_0, ..., gid_literal]
   *
   * - key_i: field-ref to the i-th grouping key in child.output when that key is active for this
   *   grouping set; null literal (of the key's type) when it is not active.
   * - passthrough columns: field-refs for non-key columns referenced by aggregate functions
   *   (same in every projection row - never null-masked).
   * - gid_literal: integer constant uniquely identifying this grouping set (bit-mask of active
   *   keys). This MUST be last so that Velox's LocalPlanner fusion check passes.
   *
   * The synthesized expand output attributes (used as [[originalInputAttributes]] for the
   * AggregateRel) mirror the above column layout.
   */
  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val inputAttrs = child.output

    // -----------------------------------------------------------------------
    // 1. Identify grouping key attributes in child.output.
    //    groupingExpressions are NamedExpressions resolving against child.output.
    //    We need the concrete AttributeReference for each key so we can:
    //      a) emit field-refs in the ExpandRel projection rows
    //      b) emit null literals with the correct type when the key is absent
    // -----------------------------------------------------------------------
    val keyAttrs: Seq[Attribute] = groupingExpressions.map {
      case attr: AttributeReference => attr
      case alias: Alias =>
        alias.child match {
          case a: AttributeReference => a
          case other =>
            // Fallback: create a synthetic attribute for the expression.
            AttributeReference(alias.name, alias.dataType, alias.nullable, alias.metadata)(
              alias.exprId,
              alias.qualifier)
        }
      case ne => ne.toAttribute
    }

    // -----------------------------------------------------------------------
    // 2. Identify aggregate input columns that are NOT grouping keys.
    //    These must be passed through unchanged in every projection row.
    // -----------------------------------------------------------------------
    val keyExprIds = keyAttrs.map(_.exprId).toSet
    val aggInputAttrs: Seq[Attribute] = aggregateExpressions
      .flatMap(_.aggregateFunction.children)
      .flatMap(_.references.toSeq)
      .distinct
      .filterNot(a => keyExprIds.contains(a.exprId))
      .flatMap(ref => inputAttrs.find(_.exprId == ref.exprId))
      .distinct

    // -----------------------------------------------------------------------
    // 3. Build ExpandRel projection rows.
    //    Column order: [key_0..key_{K-1}, passthrough_0.., gid_literal]
    //    For each grouping set gs (identified by the set of active key indices):
    //      - key_i: field-ref if i is in activeIndices, null literal otherwise
    //      - passthrough cols: always field-ref
    //      - gid: integer literal = bit-mask of active key positions (same convention as Spark)
    // -----------------------------------------------------------------------
    val numKeys = keyAttrs.size

    val projectSetExprNodes = new JArrayList[java.util.List[
      org.apache.gluten.substrait.expression.ExpressionNode]]()

    groupingSets.zipWithIndex.foreach {
      case (activeKeyIndices, gsIdx) =>
        val activeSet = activeKeyIndices.toSet
        val rowNodes = new JArrayList[org.apache.gluten.substrait.expression.ExpressionNode]()

        // Key columns: field-ref or null literal.
        keyAttrs.zipWithIndex.foreach {
          case (keyAttr, ki) =>
            if (activeSet.contains(ki)) {
              // Active key: pass through as field-ref.
              rowNodes.add(
                ExpressionConverter
                  .replaceWithExpressionTransformer(keyAttr, inputAttrs)
                  .doTransform(context))
            } else {
              // Inactive key: null literal of the key's type.
              rowNodes.add(
                ExpressionBuilder.makeLiteral(null, keyAttr.dataType, nullable = true))
            }
        }

        // Passthrough aggregate input columns (same in every row).
        aggInputAttrs.foreach { attr =>
          rowNodes.add(
            ExpressionConverter
              .replaceWithExpressionTransformer(attr, inputAttrs)
              .doTransform(context))
        }

        // Grouping ID literal - MUST be last for Velox's LocalPlanner fusion check.
        // Compute gid as a bit-mask: bit i = 1 means key i is ABSENT (Spark's convention).
        val gid: Int = (0 until numKeys).foldLeft(0) {
          case (acc, i) => if (!activeSet.contains(i)) acc | (1 << i) else acc
        }
        rowNodes.add(ExpressionBuilder.makeLiteral(gid, IntegerType, nullable = false))

        projectSetExprNodes.add(rowNodes)
    }

    val expandOperatorId = context.nextOperatorId("ExpandForMultiGroupingSet")
    val expandRel =
      RelBuilder.makeExpandRel(childCtx.root, projectSetExprNodes, context, expandOperatorId)

    // -----------------------------------------------------------------------
    // 4. Synthesize the expand output attributes so that AggregateRel can
    //    resolve groupingExpressions and aggregate function inputs against them.
    //    Layout mirrors the ExpandRel columns: [keys (nullable), passthroughs, gid].
    // -----------------------------------------------------------------------
    val expandOutputAttrs: Seq[Attribute] =
      keyAttrs.map(k => k.withNullability(true)) ++
        aggInputAttrs ++
        Seq(AttributeReference("spark_groupingid", IntegerType, nullable = false)())

    // -----------------------------------------------------------------------
    // 5. Build AggregateRel.
    //    originalInputAttributes = expandOutputAttrs (the synthesized expand output).
    //    groupingExpressions reference key attrs - those are in expandOutputAttrs by exprId.
    //    Aggregate function inputs reference aggInputAttrs - also in expandOutputAttrs.
    // -----------------------------------------------------------------------
    val aggParams = new AggregationParams
    val aggOperatorId = context.nextOperatorId(this.nodeName)
    val aggRel = getAggRel(context, aggOperatorId, aggParams, expandRel, validation = false)

    context.registerAggregationParam(aggOperatorId, aggParams)
    TransformContext(output, aggRel)
  }

  /**
   * Override getAggRel to use expandOutputAttrs as the input attribute set rather than
   * child.output.
   */
  override protected def getAggRel(
      context: SubstraitContext,
      operatorId: Long,
      aggParams: AggregationParams,
      input: RelNode,
      validation: Boolean): RelNode = {
    // Reconstruct expandOutputAttrs in the same way doTransform does.
    val inputAttrs = child.output
    val keyAttrs: Seq[Attribute] = groupingExpressions.map {
      case attr: AttributeReference => attr
      case alias: Alias =>
        alias.child match {
          case a: AttributeReference => a
          case _ =>
            AttributeReference(alias.name, alias.dataType, alias.nullable, alias.metadata)(
              alias.exprId,
              alias.qualifier)
        }
      case ne => ne.toAttribute
    }
    val keyExprIds = keyAttrs.map(_.exprId).toSet
    val aggInputAttrs: Seq[Attribute] = aggregateExpressions
      .flatMap(_.aggregateFunction.children)
      .flatMap(_.references.toSeq)
      .distinct
      .filterNot(a => keyExprIds.contains(a.exprId))
      .flatMap(ref => inputAttrs.find(_.exprId == ref.exprId))
      .distinct

    val expandOutputAttrs: Seq[Attribute] =
      keyAttrs.map(k => k.withNullability(true)) ++
        aggInputAttrs ++
        Seq(AttributeReference("spark_groupingid", IntegerType, nullable = false)())

    // Delegate to the parent's logic but override originalInputAttributes via a wrapper
    // by calling the internal helper directly. Since getAggRelInternal is private to the
    // parent, we call the parent getAggRel but ensure child.output is already correct.
    // Instead, we replicate the logic here using the synthesized expandOutputAttrs.
    getAggRelWithInputAttrs(context, expandOutputAttrs, operatorId, input, validation, aggParams)
  }

  private def getAggRelWithInputAttrs(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean,
      aggParams: AggregationParams): RelNode = {
    import org.apache.gluten.expression.ConverterUtils.FunctionConfig
    import org.apache.gluten.substrait.expression.{AggregateFunctionNode, ExpressionNode}
    import org.apache.spark.sql.catalyst.expressions.aggregate._
    import org.apache.gluten.utils.VeloxIntermediateData
    import scala.collection.JavaConverters._

    val groupingList = groupingExpressions.map { expr =>
      ExpressionConverter
        .replaceWithExpressionTransformer(expr, originalInputAttributes)
        .doTransform(context)
    }.asJava

    val aggFilterList = new JArrayList[ExpressionNode]()
    val aggregateFunctionList = new JArrayList[AggregateFunctionNode]()

    aggregateExpressions.foreach { aggExpr =>
      aggFilterList.add(
        aggExpr.filter
          .map(
            ExpressionConverter
              .replaceWithExpressionTransformer(_, originalInputAttributes)
              .doTransform(context))
          .orNull)

      val aggregateFunc = aggExpr.aggregateFunction
      val childrenNodes = aggExpr.mode match {
        case Partial | Complete =>
          aggregateFunc.children.toList.map { c =>
            ExpressionConverter
              .replaceWithExpressionTransformer(c, originalInputAttributes)
              .doTransform(context)
          }
        case PartialMerge | Final =>
          aggregateFunc.inputAggBufferAttributes.map { attr =>
            ExpressionConverter
              .replaceWithExpressionTransformer(attr, originalInputAttributes)
              .doTransform(context)
          }
      }
      aggregateFunctionList.add(
        makeFunctionNode(context, aggregateFunc, childrenNodes.asJava, aggExpr.mode))
    }

    RelBuilder.makeAggregateRel(
      input,
      groupingList,
      aggregateFunctionList,
      aggFilterList,
      context,
      operatorId)
  }
}
