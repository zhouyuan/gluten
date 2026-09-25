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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._

/**
 * Replaces an Expand followed by a partial aggregation, as planned for ROLLUP or for grouping sets
 * that each are a subset of the previous one, with a [[RollupHashAggregateExecTransformer]].
 *
 * {{{
 *   HashAggregate(keys=[k1, k2, gid], functions=[partial_sum(_pre_0)])
 *   +- Project [k1, k2, gid, (x * y) AS _pre_0]      // optional pre-projection
 *      +- Expand [[x, y, a, b, 0], [x, y, a, null, 1], [x, y, null, null, 3]], [x, y, k1, k2, gid]
 *         +- child
 * }}}
 * becomes
 * {{{
 *   RollupHashAggregate(keys=[k1, k2, gid], functions=[partial_sum(_pre_0)],
 *                       groupingSets=[0:[a, b], 1:[a], 3:[]])
 *   +- Project [a, b, (x * y) AS _pre_0]              // only if there was a pre-projection
 *      +- child
 * }}}
 *
 * The Expand copies every input row once per grouping set, while the rollup aggregation computes
 * every grouping set from the partial results of the previous one.
 */
case class RollupAggregationRule(session: SparkSession) extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!VeloxConfig.get.enableRollupAggregation) {
      return plan
    }
    plan.transformUp {
      case agg: RegularHashAggregateExecTransformer =>
        toRollupAggregate(agg, agg.initialInputBufferOffset).getOrElse(agg)
      case agg: FlushableHashAggregateExecTransformer =>
        toRollupAggregate(agg, agg.initialInputBufferOffset).getOrElse(agg)
      // E.g. min / max over strings is planned as a sort aggregate.
      case agg: SortHashAggregateExecTransformer =>
        toRollupAggregate(agg, agg.initialInputBufferOffset).getOrElse(agg)
    }
  }

  private def toRollupAggregate(
      agg: HashAggregateExecTransformer,
      initialInputBufferOffset: Int): Option[SparkPlan] = {
    val (preProject, expand) = agg.child match {
      case expand: ExpandExecTransformer => (None, expand)
      case project @ ProjectExecTransformer(_, expand: ExpandExecTransformer) =>
        (Some(project), expand)
      case _ => return None
    }
    if (!isSupportedAggregate(agg) || expand.projections.size < 2) {
      return None
    }

    // Columns the Expand passes through unchanged from its child.
    val childOutput = AttributeSet(expand.child.output)
    val passThrough = AttributeSet(expand.output.zipWithIndex.collect {
      case (attr, i)
          if childOutput.contains(attr) &&
            expand.projections.forall(_(i).semanticEquals(attr)) =>
        attr
    })

    // The grouping id must be the last grouping key, as in the output of the native aggregation.
    val groupingAttributes = agg.groupingExpressions.map {
      case attr: Attribute => attr
      case _ => return None
    }
    val groupIdIndex = expand.output.indexWhere(_.semanticEquals(groupingAttributes.last))
    if (groupIdIndex < 0) {
      return None
    }
    val groupIds = expand.projections.map {
      _(groupIdIndex) match {
        case Literal(value: Long, LongType) => value
        case Literal(value: Int, IntegerType) => value.toLong
        case _ => return None
      }
    }
    if (groupIds.distinct.size != groupIds.size) {
      return None
    }

    // Each grouping key is either null or the same child attribute in every projection.
    val keys = groupingAttributes.init.map {
      attr =>
        val index = expand.output.indexWhere(_.semanticEquals(attr))
        if (index < 0) {
          return None
        }
        val sources = expand.projections.map(_(index)).filterNot(isNullLiteral).distinct
        sources match {
          case Seq(source: Attribute) if childOutput.contains(source) => (index, source)
          case _ => return None
        }
    }

    // The aggregate functions must only read columns that are the same in every copy of a row.
    val aliases = preProject.toSeq.flatMap(_.projectList).flatMap {
      case attr: Attribute if expand.outputSet.contains(attr) => None
      case alias: Alias if alias.references.subsetOf(passThrough) => Some(alias)
      case _ => return None
    }
    val aggregateInputs = passThrough ++ AttributeSet(aliases.map(_.toAttribute))
    if (!agg.aggregateExpressions.forall(_.references.subsetOf(aggregateInputs))) {
      return None
    }

    // The grouping sets, finest first, must each be a subset of the previous one.
    val groupingSets = expand.projections
      .map(projection => keys.indices.filterNot(k => isNullLiteral(projection(keys(k)._1))))
      .zip(groupIds)
      .sortBy(-_._1.size)
    val isChain = groupingSets.sliding(2).forall {
      case Seq((finer, _), (coarser, _)) => coarser.toSet.subsetOf(finer.toSet)
      case _ => true
    }
    if (!isChain) {
      return None
    }

    val keySources = keys.map(_._2)
    val newChild = preProject match {
      case Some(project) =>
        val required = AttributeSet(keySources) ++ AttributeSet(aliases.flatMap(_.references)) ++
          AttributeSet(agg.aggregateExpressions.flatMap(_.references)).intersect(passThrough)
        val newProject = ProjectExecTransformer(
          expand.child.output.filter(required.contains) ++ aliases,
          expand.child)
        newProject.copyTagsFrom(project)
        if (!newProject.doValidate().ok()) {
          return None
        }
        newProject
      case None => expand.child
    }

    val rollup = RollupHashAggregateExecTransformer(
      agg.requiredChildDistributionExpressions,
      agg.groupingExpressions,
      agg.aggregateExpressions,
      agg.aggregateAttributes,
      initialInputBufferOffset,
      agg.resultExpressions,
      newChild,
      keySources,
      groupingSets.map(_._1),
      groupingSets.map(_._2)
    )
    rollup.copyTagsFrom(agg)
    val validation = rollup.doValidate()
    if (!validation.ok()) {
      logDebug(s"Rollup aggregation is not supported: ${validation.reason()}")
      return None
    }
    Some(rollup)
  }

  private def isNullLiteral(expr: Expression): Boolean = expr match {
    case Literal(null, _) => true
    case _ => false
  }

  private def isSupportedAggregate(agg: HashAggregateExecTransformer): Boolean = {
    agg.aggregateExpressions.nonEmpty &&
    agg.aggregateExpressions.forall {
      expr =>
        expr.mode == Partial && !expr.isDistinct && expr.filter.isEmpty &&
        isSupportedFunction(expr.aggregateFunction)
    }
  }

  private def isSupportedFunction(function: AggregateFunction): Boolean = function match {
    // Like the flushable aggregation, only merge floating point sums in any order if allowed.
    case Sum(child, _) if isFloatingPoint(child.dataType) => allowLooseFloatingPoint
    case Average(child, _) if isFloatingPoint(child.dataType) => allowLooseFloatingPoint
    case s: Sum => s.prettyName == "sum"
    case _: Average | _: Count | _: Min | _: Max => true
    case _ => false
  }

  private def isFloatingPoint(dataType: DataType): Boolean =
    dataType == DoubleType || dataType == FloatType

  private def allowLooseFloatingPoint: Boolean = VeloxConfig.get.floatingPointMode == "loose"
}
