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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.metrics.{GlutenTimeMetric, IMetrics}
import org.apache.gluten.substrait.rel.SplitInfo

import org.apache.spark.{Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.SparkInputMetricsUtil.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters.asScalaBufferConverter

trait BaseGlutenPartition extends Partition with InputPartition {
  def plan: Array[Byte]
}

/**
 * Wraps Hadoop/Velox filesystem credential key-value pairs (fs.azure.*, fs.s3a.*,
 * fs.gs.*) so they cannot be accidentally exposed through logging, toString,
 * exception messages, or other debug paths that might print an arbitrary object.
 *
 * `toString` is deliberately overridden to redact values — only key NAMES are
 * shown (these are not secret; only the bearer values like access keys, secret
 * keys, and OAuth client secrets are). This makes accidental leaks structurally
 * harder: a future `logDebug(s"... $fsConfHolder")` or similar call will print
 * "FsCredentialConf(3 keys: [fs.azure.account.auth.type, ...], values redacted)"
 * instead of the literal secret strings.
 *
 * The underlying map is also `private` so it cannot be reached by name from
 * outside this file without going through `.unsafeValue`, which is named to
 * discourage casual use.
 */
final case class FsCredentialConf private (private val raw: Map[String, String]) {

  /** Number of credential entries held. Safe to log. */
  def size: Int = raw.size

  def isEmpty: Boolean = raw.isEmpty
  def nonEmpty: Boolean = raw.nonEmpty

  /**
   * Returns the underlying map with real values. Named `unsafeValue` to make
   * call sites grep-able and to discourage passing the result to logging code.
   * Only the native JNI boundary (extraConf for NativePlanEvaluator) should
   * call this.
   */
  def unsafeValue: Map[String, String] = raw

  override def toString: String =
    if (raw.isEmpty) "FsCredentialConf(empty)"
    else s"FsCredentialConf(${raw.size} keys: [${raw.keys.toSeq.sorted.mkString(", ")}], values redacted)"
}

object FsCredentialConf {
  val empty: FsCredentialConf = FsCredentialConf(Map.empty)
  def apply(raw: Map[String, String]): FsCredentialConf = new FsCredentialConf(raw)
}

case class GlutenPartition(
    index: Int,
    plan: Array[Byte],
    splitInfos: Array[SplitInfo] = Array.empty[SplitInfo],
    files: Array[String] =
      Array.empty[String] // touched files, for implementing UDF input_file_name
) extends BaseGlutenPartition {

  override def preferredLocations(): Array[String] =
    splitInfos.flatMap(_.preferredLocations().asScala)
}

case class FirstZippedPartitionsPartition(
    index: Int,
    inputPartition: Partition,
    inputColumnarRDDPartitions: Seq[Partition] = Seq.empty)
  extends Partition

class GlutenWholeStageColumnarRDD(
    @transient sc: SparkContext,
    @transient private val inputPartitions: Seq[Partition],
    var rdds: ColumnarInputRDDsWrapper,
    pipelineTime: SQLMetric,
    updateInputMetrics: InputMetricsWrapper => Unit,
    updateNativeMetrics: IMetrics => Unit,
    enableCudf: Boolean = false,
    // fsConf carries fs.azure.* / fs.s3a.* / fs.gs.* credentials captured on the
    // driver and needed by the native Velox backend on executors.
    //
    // Stored as a field on the RDD (not in GlutenPartition) so it is serialized
    // ONCE as part of the task closure — once per executor, not once per partition.
    // This avoids O(numPartitions) copies on the heap and across the network.
    // An explicit Broadcast variable is deliberately NOT used: sc.broadcast() adds
    // BlockManager registration, HTTP fetch, and master round-trip overhead on
    // every query, even when fsConf is empty (the common case for non-cloud storage).
    //
    // Wrapped in FsCredentialConf (not a plain Map) so secret values are never
    // exposed by accidental toString/logging of this RDD or its fields — only
    // key names are printable; the real values require the explicit, grep-able
    // `.unsafeValue` accessor.
    private val fsConf: FsCredentialConf = FsCredentialConf.empty)
  extends RDD[ColumnarBatch](sc, rdds.getDependencies) {

  // Override toString so that fsConf credential values are never exposed in
  // Spark logs, DAG visualization, toDebugString, or the UI. Delegates to
  // FsCredentialConf.toString, which shows only key NAMES (redacted values) —
  // see FsCredentialConf's doc comment for the full rationale.
  override def toString: String =
    s"GlutenWholeStageColumnarRDD[$id] fsConf=$fsConf"

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    GlutenTimeMetric.millis(pipelineTime) {
      _ =>
        val (inputPartition, inputColumnarRDDPartitions) = castNativePartition(split)
        val inputIterators = rdds.getIterators(inputColumnarRDDPartitions, context)
        BackendsApiManager.getIteratorApiInstance.genFirstStageIterator(
          inputPartition,
          context,
          pipelineTime,
          updateInputMetrics,
          updateNativeMetrics,
          split.index,
          inputIterators,
          enableCudf,
          fsConf.unsafeValue
        )
    }
  }

  private def castNativePartition(split: Partition): (BaseGlutenPartition, Seq[Partition]) = {
    split match {
      case FirstZippedPartitionsPartition(_, g: BaseGlutenPartition, p) => (g, p)
      case _ => throw new SparkException(s"[BUG] Not a NativeSubstraitPartition: $split")
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castNativePartition(split)._1.preferredLocations()
  }

  override protected def getPartitions: Array[Partition] = {
    inputPartitions.zipWithIndex
      .map {
        case (partition, i) => FirstZippedPartitionsPartition(i, partition, rdds.getPartitions(i))
      }
      .toArray[Partition]
  }

  override protected def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}
