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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.execution.{GlutenPartition, WholeStageTransformContext}
import org.apache.gluten.substrait.plan.PlanBuilder

import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests that [[VeloxIteratorApi.genPartitions]] captures fs.azure.*, fs.s3a.*, and fs.gs.* keys
 * from the driver-side Hadoop configuration and embeds them in [[GlutenPartition.fsConf]], so they
 * are available on executors where Spark's SQLConf propagation does not reach "fs.*" keys.
 *
 * Keys must be set on `sparkContext.hadoopConfiguration` (the mutable base configuration) because
 * `sessionState.newHadoopConf()` creates a fresh copy each time - mutations to its return value are
 * discarded before the next call.
 */
class VeloxIteratorApiFsConfSuite extends SharedSparkSession {

  private val api = new VeloxIteratorApi

  /**
   * Build a minimal WholeStageTransformContext backed by an empty Substrait plan. genPartitions
   * only calls wsCtx.root.toProtobuf.toByteArray, so a plan with no relations is sufficient for the
   * purpose of this test.
   */
  private def emptyWsCtx: WholeStageTransformContext =
    WholeStageTransformContext(PlanBuilder.empty())

  /**
   * Set Hadoop conf keys on the underlying mutable configuration and restore their previous values
   * (or unset them) after the block. `sessionState.newHadoopConf()` copies from
   * `sparkContext.hadoopConfiguration`, so this is the correct mutation point.
   */
  private def withHadoopConf(pairs: (String, String)*)(body: => Unit): Unit = {
    // scalastyle:off hadoopconfiguration
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    // scalastyle:off hadoopconfiguration
    val prev: Seq[(String, Option[String])] = pairs.map {
      case (k, _) => k -> Option(hadoopConf.get(k))
    }
    pairs.foreach { case (k, v) => hadoopConf.set(k, v) }
    try body
    finally prev.foreach {
        case (k, Some(old)) => hadoopConf.set(k, old)
        case (k, None) => hadoopConf.unset(k)
      }
  }

  test("genPartitions embeds fs.azure.* keys from Hadoop conf into GlutenPartition.fsConf") {
    withHadoopConf(
      "fs.azure.account.auth.type.myaccount.dfs.core.windows.net" -> "OAuth",
      "fs.azure.account.oauth.provider.type" -> "ClientCredentials"
    ) {
      val partitions = api.genPartitions(emptyWsCtx, Seq(Seq.empty), Seq.empty)
      assert(partitions.size == 1)
      val fsConf = partitions.head.asInstanceOf[GlutenPartition].fsConf
      assert(
        fsConf.contains("fs.azure.account.auth.type.myaccount.dfs.core.windows.net"),
        s"Expected fs.azure key not found; got: ${fsConf.keys.mkString(", ")}")
      assert(fsConf("fs.azure.account.auth.type.myaccount.dfs.core.windows.net") == "OAuth")
      assert(
        fsConf.contains("fs.azure.account.oauth.provider.type"),
        s"Expected fs.azure key not found; got: ${fsConf.keys.mkString(", ")}")
      assert(fsConf("fs.azure.account.oauth.provider.type") == "ClientCredentials")
    }
  }

  test("genPartitions embeds fs.s3a.* keys from Hadoop conf into GlutenPartition.fsConf") {
    withHadoopConf(
      "fs.s3a.access.key" -> "AKIAIOSFODNN7EXAMPLE",
      "fs.s3a.secret.key" -> "wJalrXUtnFEMI"
    ) {
      val partitions = api.genPartitions(emptyWsCtx, Seq(Seq.empty), Seq.empty)
      assert(partitions.size == 1)
      val fsConf = partitions.head.asInstanceOf[GlutenPartition].fsConf
      assert(
        fsConf.contains("fs.s3a.access.key"),
        s"Expected fs.s3a.access.key not found; got: ${fsConf.keys.mkString(", ")}")
      assert(fsConf("fs.s3a.access.key") == "AKIAIOSFODNN7EXAMPLE")
      assert(fsConf.contains("fs.s3a.secret.key"))
      assert(fsConf("fs.s3a.secret.key") == "wJalrXUtnFEMI")
    }
  }

  test("genPartitions embeds fs.gs.* keys from Hadoop conf into GlutenPartition.fsConf") {
    withHadoopConf("fs.gs.auth.service.account.json.keyfile" -> "/tmp/sa.json") {
      val partitions = api.genPartitions(emptyWsCtx, Seq(Seq.empty), Seq.empty)
      assert(partitions.size == 1)
      val fsConf = partitions.head.asInstanceOf[GlutenPartition].fsConf
      assert(
        fsConf.contains("fs.gs.auth.service.account.json.keyfile"),
        s"Expected fs.gs key not found; got: ${fsConf.keys.mkString(", ")}")
      assert(fsConf("fs.gs.auth.service.account.json.keyfile") == "/tmp/sa.json")
    }
  }

  test("genPartitions does not include non-fs.* keys in GlutenPartition.fsConf") {
    withHadoopConf(
      "fs.s3a.access.key" -> "KEY",
      "mapreduce.input.fileinputformat.split.maxsize" -> "128000000"
    ) {
      val partitions = api.genPartitions(emptyWsCtx, Seq(Seq.empty), Seq.empty)
      val fsConf = partitions.head.asInstanceOf[GlutenPartition].fsConf
      assert(
        !fsConf.contains("mapreduce.input.fileinputformat.split.maxsize"),
        "Non-fs key must not appear in fsConf")
      // fs.s3a.access.key must be captured
      assert(
        fsConf.contains("fs.s3a.access.key"),
        s"Expected fs.s3a.access.key not found; got: ${fsConf.keys.mkString(", ")}")
    }
  }

  test("genPartitions produces one partition per input split group") {
    withHadoopConf("fs.s3a.endpoint" -> "s3.amazonaws.com") {
      // Two split groups => two GlutenPartitions
      val partitions =
        api.genPartitions(emptyWsCtx, Seq(Seq.empty, Seq.empty), Seq.empty)
      assert(partitions.size == 2)
      partitions.foreach {
        p =>
          val fsConf = p.asInstanceOf[GlutenPartition].fsConf
          assert(
            fsConf.contains("fs.s3a.endpoint"),
            s"Expected fs.s3a.endpoint not found; got: ${fsConf.keys.mkString(", ")}")
          assert(fsConf("fs.s3a.endpoint") == "s3.amazonaws.com")
      }
    }
  }
}
