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
package org.apache.gluten.extension.joinagg

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualNullSafe, EqualTo, Expression, ExprId}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable

/**
 * Equality and cardinality reasoning shared by [[PushAggregateThroughJoin]], which decides whether
 * a pushed pre-aggregation is worth planting, and [[ImplementJoinAggregate]], which decides whether
 * a local pre-shuffle merge above the join can still remove rows.
 */
private[joinagg] object JoinAggregateCardinality {

  /**
   * Attribute equalities that hold for every row an aggregate above `plan` sees. Only inner and
   * left-semi equi-joins qualify: an outer join null-extends one side, so its keys are not
   * interchangeable.
   */
  def equiJoinEqualities(plan: LogicalPlan): Map[ExprId, Set[ExprId]] = {
    val edges = mutable.HashMap.empty[ExprId, mutable.HashSet[ExprId]]
    def addEdge(left: ExprId, right: ExprId): Unit = {
      edges.getOrElseUpdate(left, mutable.HashSet.empty) += right
      edges.getOrElseUpdate(right, mutable.HashSet.empty) += left
    }
    plan.foreach {
      case join: Join if join.joinType == Inner || join.joinType == LeftSemi =>
        join.condition.toSeq.flatMap(splitConjunction).foreach {
          case EqualTo(left: Attribute, right: Attribute) => addEdge(left.exprId, right.exprId)
          case EqualNullSafe(left: Attribute, right: Attribute) =>
            addEdge(left.exprId, right.exprId)
          case _ =>
        }
      case _ =>
    }
    edges.map { case (id, neighbours) => id -> neighbours.toSet }.toMap
  }

  /** Transitive closure of `ids` over the given equality graph. */
  def expandThroughEqualities(
      ids: Set[ExprId],
      equalities: Map[ExprId, Set[ExprId]]): Set[ExprId] = {
    val seen = mutable.HashSet.empty[ExprId] ++ ids
    val pending = mutable.Queue.empty[ExprId] ++ ids
    while (pending.nonEmpty) {
      equalities.getOrElse(pending.dequeue(), Set.empty).foreach {
        neighbour => if (seen.add(neighbour)) pending.enqueue(neighbour)
      }
    }
    seen.toSet
  }

  def splitConjunction(expr: Expression): Seq[Expression] = expr match {
    case And(left, right) => splitConjunction(left) ++ splitConjunction(right)
    case other => other :: Nil
  }

  /**
   * Row count of `plan`, or None when the plan carries no usable size information at all.
   *
   * `rowCount` is only populated by the cost-based estimators and by leaves that report it - for
   * instance a v2 relation whose scan reports `numRows`, which then survives Project / Filter
   * unchanged. When it is missing, derive a count from `sizeInBytes`, which every relation reports,
   * unless `sizeInBytes` is itself the "no statistics" sentinel.
   */
  def estimatedRowCount(plan: LogicalPlan): Option[BigInt] = {
    val stats = plan.stats
    stats.rowCount.orElse {
      if (stats.sizeInBytes >= BigInt(SQLConf.get.defaultSizeInBytes)) {
        None
      } else {
        Some((stats.sizeInBytes / sizePerRow(plan.output)).max(BigInt(1)))
      }
    }
  }

  /**
   * Upper bounds on the number of distinct values each attribute of `plan` can take.
   *
   * Two sources feed this, tightest wins:
   *   - the distinct counts in `attributeStats`, which only exist for analyzed tables with
   *     `spark.sql.cbo.enabled`;
   *   - the row count of the smallest subtree that outputs the attribute. This is sound for any
   *     attribute and tight for a dimension key column, which has at most one value per dimension
   *     row - and therefore, via [[equiJoinEqualities]], tight for the fact-side foreign key joined
   *     to it. It is loose for a plain dimension attribute such as `i_category`, whose distinct
   *     count is far below the dimension's row count.
   */
  def distinctCountUpperBounds(plan: LogicalPlan): Map[ExprId, BigInt] = {
    val bounds = mutable.HashMap.empty[ExprId, BigInt]
    def tighten(id: ExprId, bound: BigInt): Unit = {
      if (bound > 0 && bounds.get(id).forall(bound < _)) {
        bounds(id) = bound
      }
    }
    plan.foreach {
      node =>
        estimatedRowCount(node).foreach(rows => node.output.foreach(a => tighten(a.exprId, rows)))
        node.stats.attributeStats.foreach {
          case (attr, colStat) => colStat.distinctCount.foreach(tighten(attr.exprId, _))
        }
    }
    bounds.toMap
  }

  /**
   * Upper bound on the number of distinct values `key` takes, using the bound of any attribute it
   * is equi-join-equal to as well as its own. Falls back to `cap` when nothing is known.
   */
  def distinctCountUpperBound(
      key: Attribute,
      equalities: Map[ExprId, Set[ExprId]],
      bounds: Map[ExprId, BigInt],
      cap: BigInt): BigInt = {
    val equalIds = equalities.getOrElse(key.exprId, Set.empty[ExprId]) + key.exprId
    val known = equalIds.flatMap(bounds.get)
    if (known.isEmpty) cap else known.min.min(cap)
  }

  /**
   * Upper bound on the number of groups `keys` form over `rowCount` rows. Stops multiplying once
   * the bound saturates at the row count, both to keep the arithmetic small and because a saturated
   * bound already means "may not reduce at all".
   */
  def groupCountUpperBound(
      keys: Seq[Attribute],
      rowCount: BigInt,
      equalities: Map[ExprId, Set[ExprId]],
      bounds: Map[ExprId, BigInt]): BigInt = {
    keys
      .foldLeft(BigInt(1)) {
        case (acc, key) =>
          if (acc >= rowCount) {
            acc
          } else {
            acc * distinctCountUpperBound(key, equalities, bounds, rowCount)
          }
      }
      .min(rowCount)
      .max(BigInt(1))
  }

  private def sizePerRow(output: Seq[Attribute]): BigInt = {
    // Mirrors EstimationUtils.getSizePerRow: a fixed per-row overhead plus the default size of each
    // output column.
    BigInt(8) + output.map(a => BigInt(a.dataType.defaultSize)).sum
  }
}
