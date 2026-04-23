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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution._

/**
 * Hash aggregation transformer that maps to Velox's MultiGroupingSetHashAggregation for efficient
 * processing of multiple grouping sets (GROUPING SETS/ROLLUP/CUBE) with a shared hash table.
 *
 * The child of this node must be an [[ExpandExecTransformer]]. During Substrait plan generation,
 * this node's [[doTransform]] (inherited from the parent) emits an ExpandRel followed by an
 * AggregateRel. Velox's SubstraitToVeloxPlan converts these into an ExpandNode sourcing an
 * AggregationNode, and Velox's internal execution engine then fuses the pair into a single
 * MultiGroupingSetHashAggregation operator (controlled by the `inline_grouping_sets_aggregation`
 * Velox query config, default true).
 *
 * @param requiredChildDistributionExpressions
 *   Required child distribution expressions
 * @param groupingExpressions
 *   Grouping expressions for the aggregation (reference the ExpandExecTransformer's output)
 * @param aggregateExpressions
 *   Aggregate expressions
 * @param aggregateAttributes
 *   Aggregate attributes
 * @param initialInputBufferOffset
 *   Initial input buffer offset
 * @param resultExpressions
 *   Result expressions
 * @param child
 *   The ExpandExecTransformer child (must be an ExpandExecTransformer)
 * @param groupingSets
 *   Metadata: the grouping sets extracted from the expand (each set is a list of grouping key
 *   indices). Used for display purposes only.
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

  // Flushing is not applicable for MultiGroupingSetHashAggregation.
  override protected def allowFlush: Boolean = false

  override def simpleString(maxFields: Int): String =
    s"MultiGroupingSet${super.simpleString(maxFields)}"

  override def verboseString(maxFields: Int): String =
    s"MultiGroupingSet${super.verboseString(maxFields)} groupingSets=$groupingSets"

  override protected def withNewChildInternal(newChild: SparkPlan): HashAggregateExecTransformer =
    copy(child = newChild)

  // doTransform is intentionally inherited from HashAggregateExecTransformer.
  // The child is an ExpandExecTransformer, so the inherited doTransform will:
  //   1. Transform the ExpandExecTransformer child → ExpandRel
  //   2. Build an AggregateRel on top of the ExpandRel
  // Velox's SubstraitToVeloxPlan converts this to ExpandNode → AggregationNode,
  // which Velox's execution engine fuses into MultiGroupingSetHashAggregation.
}
