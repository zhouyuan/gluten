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
package org.apache.gluten.config

/**
 * Forces class-initialization of every Gluten config registry object so that all ConfigEntries are
 * registered in Spark's global SQLConf.sqlConfEntries map before user code calls
 * spark.conf.isModifiable().
 *
 * Background: Scala companion objects are lazily initialized on first access. Gluten's ConfigEntry
 * values (e.g. GLUTEN_ENABLED, COLUMNAR_FILTER_ENABLED) are `val`s inside those objects. Until the
 * object is first touched, its initializer never runs, the ConfigEntry is never created, and
 * therefore SQLConf.buildConf()'s onCreate callback never fires. The callback is what inserts the
 * key into SQLConf.sqlConfEntries, which is the only thing that SQLConf.isModifiable() checks.
 *
 * Call GlutenConfigRegistries.ensureInitialized() once at plugin startup (i.e. from
 * GlutenPlugin.DriverPlugin.init()) to guarantee that all Gluten keys are visible to
 * spark.conf.isModifiable() for the lifetime of the JVM.
 */
object GlutenConfigRegistries {

  /**
   * Touch every config object so their static initializers run. The discard `val _ =` pattern
   * forces Scala to evaluate the object reference, which triggers its initialization if not yet
   * done.
   */
  def ensureInitialized(): Unit = {
    // Core configs
    val _1 = GlutenCoreConfig.getClass
    // Substrait-specific configs
    val _2 = GlutenConfig.getClass
    // Force the companion objects themselves (holds the ConfigEntry vals).
    // Accessing .getClass on the object is sufficient to trigger <clinit>.
    touchObject(GlutenCoreConfig)
    touchObject(GlutenConfig)
  }

  // Accepts Any so callers don't need to import every config type.
  @inline private def touchObject(obj: AnyRef): Unit = {
    val _ = obj.getClass.getName // triggers <clinit> if not yet run
  }
}
