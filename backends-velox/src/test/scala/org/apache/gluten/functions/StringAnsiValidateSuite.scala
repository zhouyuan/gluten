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
package org.apache.gluten.functions

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.ProjectExecTransformer

import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.sql.internal.SQLConf

class StringAnsiValidateSuite extends FunctionsValidateSuite {

  disableFallbackCheck

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(SQLConf.ANSI_ENABLED.key, "true")
  }

  test("elt") {
    // int_field1 is 1, 2, 3, so every index is within range here.
    runQueryAndCompare("SELECT elt(int_field1, 'a', 'b', 'c') FROM datatab") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    // A NULL index gives NULL rather than an error, and a NULL selected input stays NULL.
    runQueryAndCompare("SELECT elt(NULL, 'a', 'b'), elt(1, string_field1, 'b') FROM datatab") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    // An out-of-range index raises an error in ANSI mode. int_field1 - 1 is 0 for the
    // first row, and int_field1 + 3 is beyond the number of inputs for every row.
    intercept[SparkException] {
      sql("SELECT elt(int_field1 - 1, 'a', 'b', 'c') FROM datatab").collect()
    }
    intercept[SparkException] {
      sql("SELECT elt(int_field1 + 3, 'a', 'b', 'c') FROM datatab").collect()
    }
  }
}
