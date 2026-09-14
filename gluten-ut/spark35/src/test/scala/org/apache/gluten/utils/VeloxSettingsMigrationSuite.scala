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

import org.apache.gluten.utils.velox.{VeloxTestSettings, VeloxTestSettingsLegacy}

import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST

import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets

import scala.io.Source

/**
 * Proves that the data-driven [[VeloxTestSettings]] makes exactly the same run/skip decision as the
 * hand-written [[VeloxTestSettingsLegacy]] it replaces.
 *
 * Needs no SparkSession and no native library, so it runs anywhere. Delete this suite together with
 * `VeloxTestSettingsLegacy` once the migration is accepted.
 */
class VeloxSettingsMigrationSuite extends AnyFunSuite {

  private val legacy = new VeloxTestSettingsLegacy
  private val dataDriven = new VeloxTestSettings

  test("the same suites are enabled and disabled") {
    assert(dataDriven.enabledSuiteNames === legacy.enabledSuiteNames)
    assert(dataDriven.disabledSuiteNames === legacy.disabledSuiteNames)
  }

  test("every suite named in the settings file resolves to a real class") {
    // DataFileBackendTestSettings already fails construction on an unknown suite; assert the
    // catalog side too, so a suite dropped from suites.txt cannot go unnoticed.
    val loader = Thread.currentThread().getContextClassLoader
    val unknown = (dataDriven.enabledSuiteNames ++ dataDriven.disabledSuiteNames).filterNot {
      name =>
        try {
          // scalastyle:off classforname
          Class.forName(name, false, loader) != null
          // scalastyle:on classforname
        } catch {
          case _: ClassNotFoundException | _: NoClassDefFoundError => false
        }
    }
    assert(unknown === Set.empty[String])
  }

  test("the same run/skip decision for every test name the settings mention") {
    // Candidates are drawn from the settings file itself: every name and prefix it mentions
    // exercises one rule, and the synthetic names cover the default (unmentioned) case.
    val synthetic = Seq("no such test", GLUTEN_TEST + "no such test")
    val mismatches = for {
      suite <- (legacy.enabledSuiteNames ++ legacy.disabledSuiteNames).toSeq.sorted
      testName <- candidateTestNames.getOrElse(suite, Seq.empty) ++ synthetic
      if legacy.shouldRun(suite, testName) != dataDriven.shouldRun(suite, testName)
    } yield s"$suite#$testName: legacy=${legacy.shouldRun(suite, testName)}"

    assert(mismatches === Seq.empty[String], s"${mismatches.size} differing decision(s)")
  }

  /** suite -> every test name (and prefix, plus a prefix extension) the settings file mentions. */
  private lazy val candidateTestNames: Map[String, Seq[String]] = {
    val resource = "gluten-ut/settings-velox.txt"
    val stream = Thread.currentThread().getContextClassLoader.getResourceAsStream(resource)
    assert(stream != null, s"$resource is not on the test classpath")
    val source = Source.fromInputStream(stream, StandardCharsets.UTF_8.name())
    try {
      source
        .getLines()
        .map(_.trim)
        .filterNot(line => line.isEmpty || line.startsWith("#"))
        .flatMap {
          line =>
            val (directive, rest) = line.span(!_.isWhitespace) match {
              case (d, r) => (d, r.trim)
            }
            val glutenPrefixed = directive.contains("Gluten")
            rest.indexOf('#') match {
              case -1 => Nil
              case i =>
                val suite = rest.substring(0, i)
                val raw = rest.substring(i + 1)
                val name = if (glutenPrefixed) GLUTEN_TEST + raw else raw
                // For a prefix rule, also probe a name that extends it and one that does not.
                Seq(suite -> name, suite -> (name + " (extended)"))
            }
        }
        .toList
        .groupBy(_._1)
        .mapValues(_.map(_._2).distinct)
        .map(identity)
    } finally {
      source.close()
    }
  }
}
