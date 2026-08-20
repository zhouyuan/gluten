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
import org.apache.gluten.extension.OffloadDataFusionScan
import org.apache.gluten.extension.injector.Injector
import org.apache.gluten.jni.JniWorkspace

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Adds DataFusion-backed scan offload on top of the Velox backend. The native library
 * (libgluten_datafusion) is loaded eagerly on driver/executor start so a scan enabled mid-session
 * doesn't fail; it is a small self-contained cdylib.
 */
class VeloxDataFusionComponent extends Component {
  import VeloxDataFusionComponent._

  override def name(): String = "velox-datafusion"

  override def dependencies(): Seq[Class[_ <: Component]] = classOf[VeloxBackend] :: Nil

  override def injectRules(injector: Injector): Unit = {
    OffloadDataFusionScan.inject(injector)
  }

  override def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = loadNativeLib()

  override def onExecutorStart(pc: PluginContext): Unit = loadNativeLib()
}

object VeloxDataFusionComponent extends Logging {
  private val loadAttempted = new AtomicBoolean(false)
  @volatile private var loaded = false

  /** Whether libgluten_datafusion is available in this JVM. */
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
          .load(s"$platformLibDir/${System.mapLibraryName("gluten_datafusion")}")
        loaded = true
      } catch {
        case e: Exception =>
          logWarning(
            "Failed to load libgluten_datafusion; DataFusion scan offload stays disabled. " +
              "Build it with dev/builddeps-veloxbe.sh --enable_datafusion=ON.",
            e)
      }
    }
  }
}
