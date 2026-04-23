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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.{AggregationParams, SubstraitContext}
import org.apache.gluten.substrait.expression.{AggregateFunctionNode, ExpressionNode}
import org.apache.gluten.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution._

import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConverters._

/**
 * Hash aggregation that uses Velox's MultiGroupingSetHashAggregation for efficient processing of
 * multiple grouping sets (GROUPING SETS/ROLLUP/CUBE) with a shared hash table. This replaces the
 * standard Expand-then-Aggregate pattern with a more efficient implementation.
 *
 * @param requiredChildDistributionExpressions
 *   Required child distribution expressions
 * @param groupingExpressions
 *   Grouping expressions for the aggregation
 * @param aggregateExpressions
 *   Aggregate expressions
 * @param aggregateAttributes
 *   Aggregate attributes
 * @param initialInputBufferOffset
 *   Initial input buffer offset
 * @param resultExpressions
 *   Result expressions
 * @param child
 *   Child plan
 * @param groupingSets
 *   The grouping sets to compute (each set is a list of grouping expression indices)
 */
case class MultiGroupingSetHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan,
    groupingSets: Seq[Seq[Int]])
  extends HashAggregateExecTransformer(
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

  override protected def withNewChildInternal(newChild: SparkPlan): HashAggregateExecTransformer = {
    copy(child = newChild)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)

    val aggParams = new AggregationParams
    val operatorId = context.nextOperatorId(this.nodeName)

    // Build the aggregation relation with grouping sets information
    val relNode =
      getAggRelWithGroupingSets(context, operatorId, aggParams, childCtx.root, groupingSets)
    TransformContext(output, relNode)
  }

  /**
   * Build aggregation relation with grouping sets support. This method extends the base getAggRel
   * to include grouping sets information in the Substrait plan via an advanced extension node.
   */
  private def getAggRelWithGroupingSets(
      context: SubstraitContext,
      operatorId: Long,
      aggParams: AggregationParams,
      input: RelNode,
      groupingSets: Seq[Seq[Int]]): RelNode = {

    val originalInputAttributes = child.output
    val groupingList = new JArrayList[ExpressionNode]()
    groupingExpressions.foreach(
      expr => {
        groupingList.add(
          ExpressionConverter
            .replaceWithExpressionTransformer(expr, originalInputAttributes)
            .doTransform(context))
      })

    // Get the aggregate function nodes
    val aggFilterList = new JArrayList[ExpressionNode]()
    val aggregateFunctionList = new JArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(
      aggExpr => {
        if (aggExpr.filter.isDefined) {
          val filterExpr = ExpressionConverter
            .replaceWithExpressionTransformer(aggExpr.filter.get, originalInputAttributes)
            .doTransform(context)
          aggFilterList.add(filterExpr)
        } else {
          aggFilterList.add(null)
        }

        val aggregateFunc = aggExpr.aggregateFunction
        val childrenNodes = aggExpr.mode match {
          case Partial | Complete =>
            aggregateFunc.children.toList.map(
              expr =>
                ExpressionConverter
                  .replaceWithExpressionTransformer(expr, originalInputAttributes)
                  .doTransform(context))
          case other =>
            throw new GlutenNotSupportException(s"$other not supported in MultiGroupingSet.")
        }
        // Use the parent class's method to create aggregate function node
        val aggFuncNode =
          super.makeFunctionNode(context, aggregateFunc, childrenNodes.asJava, aggExpr.mode)
        aggregateFunctionList.add(aggFuncNode)
      })

    // Create advanced extension node with grouping sets information
    val extensionNode = getAdvancedExtensionWithGroupingSets(groupingSets, originalInputAttributes)

    // Build the aggregate relation with grouping sets
    RelBuilder.makeAggregateRel(
      input,
      groupingList,
      aggregateFunctionList,
      aggFilterList,
      extensionNode,
      context,
      operatorId)
  }

  /**
   * Create an advanced extension node that includes grouping sets information. The grouping sets
   * are encoded in the optimization field of the extension.
   */
  private def getAdvancedExtensionWithGroupingSets(
      groupingSets: Seq[Seq[Int]],
      originalInputAttributes: Seq[Attribute]): AdvancedExtensionNode = {

    // Encode grouping sets as a string: "grouping_sets:[[0,1,2],[0,1],[0],[]]"
    val groupingSetsStr = groupingSets.map(set => s"[${set.mkString(",")}]").mkString("[", ",", "]")
    val optimizationStr = s"grouping_sets:$groupingSetsStr"

    val optimization =
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        com.google.protobuf.StringValue.newBuilder
          .setValue(optimizationStr)
          .build)

    // Include input types for validation
    val inputTypeNodeList = originalInputAttributes
      .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      .asJava
    val enhancement =
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf)

    ExtensionBuilder.makeAdvancedExtension(optimization, enhancement)
  }
}

// Made with Bob
