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
package org.apache.gluten.utils

import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.io.Source

/**
 * A [[BackendTestSettings]] whose suite list and per-test exclusions come from a data file on the
 * test classpath, `gluten-ut/settings-<backend>.txt`, instead of from Scala code.
 *
 * The file holds one directive per line, `<directive> <suite FQN>[#<test name>]`; `#` at the start
 * of a line is a comment, and the comments directly above a directive are the reason for it. See
 * `tools/scripts/gluten-ut/ut_suites.py`, which can migrate an existing `*TestSettings.scala` into
 * this format.
 *
 * Why not Scala: the settings are pure data that has to be maintained once per Spark version per
 * backend (spark35/40/41 x velox/clickhouse/bolt), and a data file diffs cleanly between versions,
 * can be regenerated from a CI run, and can be gated against a known-failure baseline the way
 * `.github/workflows/util/delta-spark-ut` does for the Delta suite.
 *
 * Every enabled suite is checked to exist on the classpath at load time, so a typo fails fast
 * rather than silently skipping a suite.
 */
abstract class DataFileBackendTestSettings(backend: String) extends BackendTestSettings {
  import DataFileBackendTestSettings._

  loadSettings()

  private def loadSettings(): Unit = {
    val resource = s"gluten-ut/settings-$backend.txt"
    val stream = Option(Thread.currentThread().getContextClassLoader.getResourceAsStream(resource))
      .orElse(Option(getClass.getClassLoader.getResourceAsStream(resource)))
      .getOrElse(
        throw new IllegalStateException(s"Gluten test settings not on classpath: $resource"))

    val settings = new mutable.LinkedHashMap[String, SuiteSettings]
    val unknownSuites = new mutable.ArrayBuffer[String]
    try {
      val reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
      var lineNumber = 0
      var line = reader.readLine()
      while (line != null) {
        lineNumber += 1
        val where = s"$resource:$lineNumber"
        applyDirective(where, line, settings, unknownSuites)
        line = reader.readLine()
      }
    } finally {
      stream.close()
    }

    if (unknownSuites.nonEmpty) {
      throw new IllegalStateException(
        s"$resource names ${unknownSuites.size} suite(s) that do not exist on the test " +
          s"classpath. Add them to gluten-ut/suites.txt, or fix the name:\n  " +
          unknownSuites.mkString("\n  "))
    }
  }

  private def applyDirective(
      where: String,
      line: String,
      settings: mutable.Map[String, SuiteSettings],
      unknownSuites: mutable.Buffer[String]): Unit = {
    val trimmed = line.trim
    if (trimmed.isEmpty || trimmed.startsWith("#")) {
      return
    }
    val (directive, rest) = trimmed.span(!_.isWhitespace) match {
      case (d, r) => (d, r.trim)
    }
    if (rest.isEmpty) {
      throw new IllegalStateException(s"$where: '$directive' needs a suite name: $trimmed")
    }

    def suiteOf(name: String): SuiteSettings = settings.getOrElse(
      name,
      throw new IllegalStateException(
        s"$where: '$name' is not enabled; add an 'enable' line before this one"))

    // Everything after the first '#' is the test name, which may itself contain '#'.
    def split(): (String, String) = rest.indexOf('#') match {
      case -1 =>
        throw new IllegalStateException(s"$where: '$directive' needs <suite>#<test>: $trimmed")
      case i => (rest.substring(0, i), rest.substring(i + 1))
    }

    directive match {
      case "enable" =>
        if (!classExists(rest)) {
          unknownSuites += rest
        }
        settings += rest -> enableSuite(rest)
      case "disable" =>
        val (suite, reason) = rest.split(ReasonSeparator, 2) match {
          case Array(s, r) => (s.trim, r.trim)
          case _ =>
            throw new IllegalStateException(
              s"$where: 'disable' needs a reason: disable <suite> -- <reason>")
        }
        if (!classExists(suite)) {
          unknownSuites += suite
        }
        disableSuite(suite, reason)
      case "exclude" =>
        val (suite, test) = split()
        suiteOf(suite).exclude(test)
      case "excludePrefix" =>
        val (suite, prefix) = split()
        suiteOf(suite).excludeByPrefix(prefix)
      case "include" =>
        val (suite, test) = split()
        suiteOf(suite).include(test)
      case "includePrefix" =>
        val (suite, prefix) = split()
        suiteOf(suite).includeByPrefix(prefix)
      case "excludeGluten" =>
        val (suite, test) = split()
        suiteOf(suite).exclude(GLUTEN_TEST + test)
      case "includeGluten" =>
        val (suite, test) = split()
        suiteOf(suite).include(GLUTEN_TEST + test)
      case "excludeGlutenPrefix" =>
        val (suite, prefix) = split()
        suiteOf(suite).excludeByPrefix(GLUTEN_TEST + prefix)
      case "includeGlutenPrefix" =>
        val (suite, prefix) = split()
        suiteOf(suite).includeByPrefix(GLUTEN_TEST + prefix)
      case "excludeAllGluten" =>
        suiteOf(rest).excludeByPrefix(GLUTEN_TEST)
      case "includeAllGluten" =>
        suiteOf(rest).includeByPrefix(GLUTEN_TEST)
      case other =>
        throw new IllegalStateException(
          s"$where: unknown directive '$other'. Known: ${KnownDirectives.mkString(", ")}")
    }
  }
}

object DataFileBackendTestSettings {
  private val ReasonSeparator = " -- "

  private val KnownDirectives = Seq(
    "enable",
    "disable",
    "exclude",
    "excludePrefix",
    "include",
    "includePrefix",
    "excludeGluten",
    "includeGluten",
    "excludeGlutenPrefix",
    "includeGlutenPrefix",
    "excludeAllGluten",
    "includeAllGluten"
  )

  private def classExists(name: String): Boolean = {
    try {
      // Resolve without initializing: we only care that the suite is on the classpath.
      // scalastyle:off classforname
      Class.forName(name, false, Thread.currentThread().getContextClassLoader)
      // scalastyle:on classforname
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }

  /** Every suite named by an `enable` directive in the given settings resource. */
  def enabledSuiteNames(backend: String): Seq[String] = {
    val resource = s"gluten-ut/settings-$backend.txt"
    val stream = Thread.currentThread().getContextClassLoader.getResourceAsStream(resource)
    require(stream != null, s"Gluten test settings not on classpath: $resource")
    val source = Source.fromInputStream(stream, StandardCharsets.UTF_8.name())
    try {
      source
        .getLines()
        .map(_.trim)
        .filter(_.startsWith("enable "))
        .map(_.stripPrefix("enable ").trim)
        .toList
    } finally {
      source.close()
    }
  }
}
