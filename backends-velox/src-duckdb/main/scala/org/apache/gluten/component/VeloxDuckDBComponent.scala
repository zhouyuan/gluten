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
package org.apache.gluten.component

import org.apache.gluten.backendsapi.velox.VeloxBackend
import org.apache.gluten.extension.OffloadDuckDBScan
import org.apache.gluten.extension.injector.Injector
import org.apache.gluten.jni.JniWorkspace

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Adds DuckDB-backed scan offload on top of the Velox backend. The native library
 * (libgluten_duckdb) is loaded eagerly on driver/executor start so a scan enabled mid-session
 * doesn't fail; it is a self-contained shared library.
 */
class VeloxDuckDBComponent extends Component {
  import VeloxDuckDBComponent._

  override def name(): String = "velox-duckdb"

  override def dependencies(): Seq[Class[_ <: Component]] = classOf[VeloxBackend] :: Nil

  override def injectRules(injector: Injector): Unit = {
    OffloadDuckDBScan.inject(injector)
  }

  override def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = loadNativeLib()

  override def onExecutorStart(pc: PluginContext): Unit = loadNativeLib()
}

object VeloxDuckDBComponent extends Logging {
  private val loadAttempted = new AtomicBoolean(false)
  @volatile private var loaded = false

  /** Whether libgluten_duckdb is available in this JVM. */
  def nativeLibLoaded: Boolean = loaded

  private val platformLibDir: String = {
    val osName = System.getProperty("os.name") match {
      case n if n.contains("Linux") => "linux"
      case n if n.contains("Mac") => "darwin"
      case _ =>
        // Default to linux
        "linux"
    }
    val arch = System.getProperty("os.arch")
    s"$osName/$arch"
  }

  private def loadNativeLib(): Unit = {
    // Runs after VeloxBackend's lifecycle hooks (components are started in
    // dependency order), so the JniWorkspace and shared dependency libraries
    // are already in place.
    if (loadAttempted.compareAndSet(false, true)) {
      try {
        JniWorkspace.getDefault.libLoader
          .load(s"$platformLibDir/${System.mapLibraryName("gluten_duckdb")}")
        loaded = true
      } catch {
        case e: Exception =>
          logWarning(
            "Failed to load libgluten_duckdb; DuckDB scan offload stays disabled. " +
              "Build it with dev/builddeps-veloxbe.sh --enable_duckdb=ON.",
            e)
      }
    }
  }
}
