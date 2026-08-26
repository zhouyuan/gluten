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

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, EqualTo, IsNotNull, IsNull, Literal, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.{CollapseProject, ColumnPruning, RemoveNoopOperators}
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Rewrites the `left outer join ... where <right side column> is null` anti-join idiom into a real
 * `LeftAnti` join:
 *
 * {{{
 *   Filter(IsNull(r.k) AND rest, Join(l, r, LeftOuter, l.k = r.k))
 *     =>
 *   Filter(rest, Project(l.output ++ (null AS r.output), Join(l, r, LeftAnti, l.k = r.k)))
 * }}}
 *
 * TPC-DS writes several queries this way - q78 excludes returned line items with
 * `left join store_returns on ... where sr_ticket_number is null` - and vanilla Catalyst has no
 * rule for it, so the plan keeps an outer join followed by a filter. The two are equivalent
 * whenever `IsNull(r.k)` can only be true for a null-extended row, but the outer join form makes
 * the backend null-extend and materialize the right side's columns for every probe row just to
 * throw the row away, and it hides from the join that it may stop at the first match.
 *
 * `LeftAnti` drops the right side from the join output, so the rewrite re-creates those columns as
 * the nulls the outer join would have produced. That keeps the rewrite a drop-in replacement for
 * any parent plan; [[RewriteLeftOuterToLeftAntiBatch]] then prunes the ones nobody reads, which in
 * the common case is all of them.
 */
case class RewriteLeftOuterToLeftAnti(spark: SparkSession)
  extends Rule[LogicalPlan]
  with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!GlutenConfig.get.rewriteLeftOuterToLeftAntiEnabled) {
      return plan
    }
    plan.transformUp {
      case filter @ Filter(condition, join @ Join(_, right, LeftOuter, Some(_), _)) =>
        val (unmatchedChecks, remaining) = splitConjunctivePredicates(condition).partition {
          case IsNull(attr: Attribute) => nullOnlyWhenUnmatched(attr, join)
          case _ => false
        }
        if (unmatchedChecks.isEmpty) {
          filter
        } else {
          // Every row that survives `unmatchedChecks` is a row of the left side that found no
          // match on the right, which is exactly what a LeftAnti join emits.
          val rightExprIds = right.output.map(_.exprId).toSet
          val nullExtendedOutput: Seq[NamedExpression] = join.output.map {
            case attr if rightExprIds.contains(attr.exprId) =>
              Alias(Literal(null, attr.dataType), attr.name)(
                exprId = attr.exprId,
                qualifier = attr.qualifier,
                explicitMetadata = Some(attr.metadata))
            case attr => attr
          }
          val antiJoin = Project(nullExtendedOutput, join.copy(joinType = LeftAnti))
          // `remaining` may still read right side columns. It is kept above the null projection,
          // which reproduces exactly the values the outer join would have handed it.
          remaining.reduceOption(And).map(Filter(_, antiJoin)).getOrElse(antiJoin)
        }
    }
  }

  /**
   * Whether `IsNull(attr)` can only hold for a row that the outer join null-extended, which is what
   * makes discarding the matched rows equivalent to a LeftAnti join. A matched row copies this
   * column straight out of the right side, so the question is whether a row coming from the right
   * side can carry a null here at all.
   */
  private def nullOnlyWhenUnmatched(attr: Attribute, join: Join): Boolean = {
    // `Join.output` marks the right side nullable for LeftOuter, so the real nullability of the
    // column has to come from the right child rather than from the join's own output.
    join.right.output.find(_.exprId == attr.exprId).exists {
      rightAttr =>
        !rightAttr.nullable ||
        join.right.constraints.contains(IsNotNull(rightAttr)) ||
        isNullIntolerantJoinKey(rightAttr, join)
    }
  }

  /**
   * Whether the join condition itself proves the column is not null in a matched row. `EqualTo`
   * returns null - never true - when either side is null, so a row with a null equi-join key can
   * never satisfy the condition. `EqualNullSafe` deliberately does match nulls and is therefore not
   * accepted here.
   *
   * This holds no matter what the rest of the condition looks like: a match has to satisfy every
   * top level conjunct, so one null-intolerant conjunct mentioning the column is enough.
   */
  private def isNullIntolerantJoinKey(rightAttr: Attribute, join: Join): Boolean = {
    join.condition.toSeq.flatMap(splitConjunctivePredicates).exists {
      case EqualTo(left, right) =>
        left.semanticEquals(rightAttr) || right.semanticEquals(rightAttr)
      case _ => false
    }
  }
}

/**
 * Runs [[RewriteLeftOuterToLeftAnti]] and then re-runs Spark's own projection cleanup over the
 * result.
 *
 * The rewrite has to re-create the right side of the join as a projection of null literals to keep
 * the outer join's output schema. Almost no query reads those columns - the whole point of the
 * idiom is to keep the rows that have none - but by the time a user provided optimizer rule runs,
 * the batch that would have pruned them has already reached its fixed point. So run the pruning
 * again here, and only when the rewrite actually fired.
 */
case class RewriteLeftOuterToLeftAntiBatch(spark: SparkSession) extends Rule[LogicalPlan] {
  private val rewrite = RewriteLeftOuterToLeftAnti(spark)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val rewritten = rewrite(plan)
    if (rewritten.fastEquals(plan)) {
      plan
    } else {
      // Two rounds: `ColumnPruning` can leave a Project behind for `CollapseProject` /
      // `RemoveNoopOperators` to absorb, which in turn can expose one more column to prune.
      cleanup(cleanup(rewritten))
    }
  }

  private def cleanup(plan: LogicalPlan): LogicalPlan =
    RemoveNoopOperators(CollapseProject(ColumnPruning(plan)))
}
