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
package org.apache.spark.sql.hive

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.{ExpressionConverter, ExpressionTransformer, UDFMappings}

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.expression.UDFResolver

import java.util.Locale

object VeloxHiveUDFTransformer {
  def replaceWithExpressionTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    val (udfName, udfClassName) = getHiveUDFNameAndClassName(expr)

    if (UDFResolver.UDFNames.contains(udfClassName)) {
      val udfExpression = UDFResolver
        .getUdfExpression(udfClassName, udfName)(expr.children)
      udfExpression.getTransformer(
        ExpressionConverter.replaceWithExpressionTransformer(udfExpression.children, attributeSeq)
      )
    } else {
      HiveUDFTransformer.genTransformerFromUDFMappings(udfName, expr, attributeSeq)
    }
  }

  def isHiveUDFSupportsTransform(expr: Expression): Boolean = {
    val (udfName, udfClassName) = getHiveUDFNameAndClassName(expr)
    UDFResolver.UDFNames.contains(udfClassName) ||
    UDFMappings.hiveUDFMap.contains(udfName.toLowerCase(Locale.ROOT))
  }

  private def getHiveUDFNameAndClassName(expr: Expression): (String, String) = expr match {
    case s: HiveSimpleUDF =>
      (s.name.stripPrefix("default."), s.funcWrapper.functionClassName)
    case g: HiveGenericUDF =>
      (g.name.stripPrefix("default."), g.funcWrapper.functionClassName)
    case _ =>
      throw new GlutenNotSupportException(
        s"Expression $expr is not a HiveSimpleUDF or HiveGenericUDF")
  }
}
