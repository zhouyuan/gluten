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

  test("genPartitions embeds fs.azure.* keys from Hadoop conf into GlutenPartition.fsConf") {
    val hadoopConf = spark.sessionState.newHadoopConf()
    hadoopConf.set("fs.azure.account.auth.type.myaccount.dfs.core.windows.net", "OAuth")
    hadoopConf.set("fs.azure.account.oauth.provider.type", "ClientCredentials")
    try {
      val partitions = api.genPartitions(emptyWsCtx, Seq(Seq.empty), Seq.empty)
      assert(partitions.size == 1)
      val fsConf = partitions.head.asInstanceOf[GlutenPartition].fsConf
      assert(
        fsConf.contains("fs.azure.account.auth.type.myaccount.dfs.core.windows.net"),
        s"Expected fs.azure key not found; got: $fsConf")
      assert(
        fsConf("fs.azure.account.auth.type.myaccount.dfs.core.windows.net") == "OAuth")
      assert(
        fsConf.contains("fs.azure.account.oauth.provider.type"),
        s"Expected fs.azure key not found; got: $fsConf")
    } finally {
      hadoopConf.unset("fs.azure.account.auth.type.myaccount.dfs.core.windows.net")
      hadoopConf.unset("fs.azure.account.oauth.provider.type")
    }
  }

  test("genPartitions embeds fs.s3a.* keys from Hadoop conf into GlutenPartition.fsConf") {
    val hadoopConf = spark.sessionState.newHadoopConf()
    hadoopConf.set("fs.s3a.access.key", "AKIAIOSFODNN7EXAMPLE")
    hadoopConf.set("fs.s3a.secret.key", "wJalrXUtnFEMI")
    try {
      val partitions = api.genPartitions(emptyWsCtx, Seq(Seq.empty), Seq.empty)
      assert(partitions.size == 1)
      val fsConf = partitions.head.asInstanceOf[GlutenPartition].fsConf
      assert(fsConf.contains("fs.s3a.access.key"), s"Expected fs.s3a key not found; got: $fsConf")
      assert(fsConf("fs.s3a.access.key") == "AKIAIOSFODNN7EXAMPLE")
      assert(fsConf.contains("fs.s3a.secret.key"))
    } finally {
      hadoopConf.unset("fs.s3a.access.key")
      hadoopConf.unset("fs.s3a.secret.key")
    }
  }

  test("genPartitions embeds fs.gs.* keys from Hadoop conf into GlutenPartition.fsConf") {
    val hadoopConf = spark.sessionState.newHadoopConf()
    hadoopConf.set("fs.gs.auth.service.account.json.keyfile", "/tmp/sa.json")
    try {
      val partitions = api.genPartitions(emptyWsCtx, Seq(Seq.empty), Seq.empty)
      assert(partitions.size == 1)
      val fsConf = partitions.head.asInstanceOf[GlutenPartition].fsConf
      assert(
        fsConf.contains("fs.gs.auth.service.account.json.keyfile"),
        s"Expected fs.gs key not found; got: $fsConf")
      assert(fsConf("fs.gs.auth.service.account.json.keyfile") == "/tmp/sa.json")
    } finally {
      hadoopConf.unset("fs.gs.auth.service.account.json.keyfile")
    }
  }

  test("genPartitions does not include non-fs.* keys in GlutenPartition.fsConf") {
    val hadoopConf = spark.sessionState.newHadoopConf()
    hadoopConf.set("fs.s3a.access.key", "KEY")
    hadoopConf.set("spark.some.conf", "value")
    hadoopConf.set("mapreduce.input.fileinputformat.split.maxsize", "128000000")
    try {
      val partitions = api.genPartitions(emptyWsCtx, Seq(Seq.empty), Seq.empty)
      val fsConf = partitions.head.asInstanceOf[GlutenPartition].fsConf
      assert(!fsConf.contains("spark.some.conf"), "Non-fs key must not appear in fsConf")
      assert(
        !fsConf.contains("mapreduce.input.fileinputformat.split.maxsize"),
        "Non-fs key must not appear in fsConf")
      assert(fsConf.contains("fs.s3a.access.key"))
    } finally {
      hadoopConf.unset("fs.s3a.access.key")
      hadoopConf.unset("spark.some.conf")
      hadoopConf.unset("mapreduce.input.fileinputformat.split.maxsize")
    }
  }

  test("genPartitions produces empty fsConf when no fs.* keys are set") {
    // Use a key guaranteed not to exist in the Hadoop conf under any test profile.
    val hadoopConf = spark.sessionState.newHadoopConf()
    val uniqueKey = "fs.azure.__test_only_unique_key__"
    hadoopConf.unset(uniqueKey)
    // Count only keys matching our prefixes.
    val partitions = api.genPartitions(emptyWsCtx, Seq(Seq.empty), Seq.empty)
    val fsConf = partitions.head.asInstanceOf[GlutenPartition].fsConf
    assert(!fsConf.contains(uniqueKey))
  }
}
