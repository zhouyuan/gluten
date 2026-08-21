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
import org.apache.gluten.execution.DuckDBScanExec
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.columnar.validator.Validators
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}

/**
 * Replaces supported parquet [[FileSourceScanExec]]s with [[DuckDBScanExec]]. This rule is injected
 * by [[org.apache.gluten.component.VeloxDuckDBComponent]] and therefore runs before the Velox
 * backend's own offload rules; scans it doesn't claim keep going down the regular Velox scan path.
 */
case class OffloadDuckDBScan() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case scan: FileSourceScanExec =>
      DuckDBScanExec.tryOffload(scan).getOrElse(scan)
    case other => other
  }
}

object OffloadDuckDBScan {
  def inject(injector: Injector): Unit = {
    // Inject legacy rule.
    injector.gluten.legacy.injectTransform {
      c =>
        val offload = Seq(OffloadDuckDBScan())
        HeuristicTransform.Simple(
          Validators.newValidator(new GlutenConfig(c.sqlConf), offload),
          offload
        )
    }
  }
}
