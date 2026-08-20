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
import org.apache.gluten.backendsapi.arrow.ArrowBatchTypes
import org.apache.gluten.component.VeloxDataFusionComponent
import org.apache.gluten.config.{GlutenConfig, GlutenDataFusionConfig}
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.substrait.`type`.ColumnTypeNode
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.plan.PlanBuilder
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.RelBuilder

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.{FileSourceScanExec, FileSourceScanExecShim}
import org.apache.spark.sql.execution.datafusion.{DataFusionScanRDD, DataFusionScanSplit}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SparkVersionUtil
import org.apache.spark.util.collection.BitSet

import com.google.common.collect.Lists
import io.substrait.proto.NamedStruct

import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

/**
 * A parquet table scan executed by Apache DataFusion, producing Arrow-native batches. It runs as
 * its own leaf stage: downstream Velox operators consume its output through the regular
 * InputIteratorTransformer / value-stream mechanism, with the Arrow-to-Velox transition inserted
 * automatically by the transition planner.
 */
case class DataFusionScanExec(
    @transient override val relation: HadoopFsRelation,
    override val output: Seq[Attribute],
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int],
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean = false)
  extends FileSourceScanExecShim(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan)
  with GlutenPlan {

  override def batchType(): Convention.BatchType = ArrowBatchTypes.ArrowNativeBatchType

  override def rowType(): Convention.RowType = Convention.RowType.None

  @transient private lazy val executorSideMetrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "total scan time")
  )

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    executorSideMetrics ++ driverMetricsAlias

  override val nodeName: String =
    s"DataFusionScan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"

  override val nodeNamePrefix: String = ""

  def getPartitions: Seq[Partition] = {
    if (SparkVersionUtil.gteSpark40) {
      getPartitionsSeq()
    } else {
      BackendsApiManager.getTransformerApiInstance
        .genPartitionSeq(
          relation,
          requiredSchema,
          getPartitionArray,
          output,
          bucketedScan,
          optionalBucketSet,
          optionalNumCoalescedBuckets,
          disableBucketedScan,
          dataFilters
        )
    }
  }

  /** Serializes a standalone substrait plan holding this scan's single ReadRel. */
  private def buildScanPlanBytes(): Array[Byte] = {
    val context = new SubstraitContext
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(output)
    val nameList = ConverterUtils.collectAttributeNamesWithoutExprId(output)
    val columnTypeNodes = output.map {
      attr =>
        if (relation.partitionSchema.exists(_.name.equals(attr.name))) {
          new ColumnTypeNode(NamedStruct.ColumnType.PARTITION_COL)
        } else {
          new ColumnTypeNode(NamedStruct.ColumnType.NORMAL_COL)
        }
    }.asJava
    val readNode = RelBuilder.makeReadRel(
      typeNodes,
      nameList,
      /* filter */ null,
      columnTypeNodes,
      /* extension */ null,
      context,
      context.nextOperatorId(nodeName))
    PlanBuilder.makePlan(context, Lists.newArrayList(readNode), nameList).toProtobuf.toByteArray
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val planBytes = buildScanPlanBytes()
    val splits = getPartitions.map {
      partition =>
        val splitInfo = BackendsApiManager.getIteratorApiInstance.genSplitInfo(
          partition.index,
          Seq(partition),
          relation.partitionSchema,
          relation.dataSchema,
          ReadFileFormat.ParquetReadFormat,
          Seq.empty,
          Map.empty)
        DataFusionScanSplit(
          splitInfo.toProtobuf.toByteArray,
          splitInfo.preferredLocations().asScala.toArray)
    }
    val confJson =
      s"""{"batch_size":${GlutenConfig.get.maxBatchSize},""" +
        s""""threads":${GlutenDataFusionConfig.get.threads}}"""
    new DataFusionScanRDD(
      sparkContext,
      planBytes,
      splits,
      confJson.getBytes(StandardCharsets.UTF_8),
      longMetric("numOutputRows"),
      longMetric("numOutputBatches"),
      longMetric("scanTime")
    )
  }

  override def doCanonicalize(): DataFusionScanExec = {
    DataFusionScanExec(
      relation,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        partitionFilters.filterNot(isDynamicPruningFilter),
        output),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, output),
      None,
      disableBucketedScan
    )
  }
}

object DataFusionScanExec {

  /**
   * Offloads the scan to DataFusion when supported; returns None to leave it to the regular (Velox)
   * scan path.
   */
  def tryOffload(scan: FileSourceScanExec): Option[DataFusionScanExec] = {
    if (!GlutenDataFusionConfig.get.scanEnabled || !GlutenConfig.get.enableColumnarFileScan) {
      return None
    }
    if (!VeloxDataFusionComponent.nativeLibLoaded) {
      return None
    }
    if (!scan.relation.fileFormat.isInstanceOf[ParquetFileFormat]) {
      return None
    }
    val exec = DataFusionScanExec(
      scan.relation,
      scan.output,
      scan.requiredSchema,
      scan.partitionFilters,
      scan.optionalBucketSet,
      scan.optionalNumCoalescedBuckets,
      scan.dataFilters,
      scan.tableIdentifier,
      scan.disableBucketedScan
    )
    if (supports(exec)) Some(exec) else None
  }

  private val inputFileRelatedNames =
    Set("input_file_name", "input_file_block_start", "input_file_block_length")

  private def supports(exec: DataFusionScanExec): Boolean = {
    val relation = exec.relation
    def isPartitionColumn(attr: Attribute): Boolean =
      relation.partitionSchema.exists(_.name.equals(attr.name))
    // Stage-1 restrictions; anything outside falls back to the Velox scan.
    exec.output.nonEmpty &&
    exec.output.exists(!isPartitionColumn(_)) &&
    !exec.bucketedScan &&
    exec.metadataColumns.isEmpty &&
    !exec.hasFieldIds &&
    exec.output.forall(attr => !inputFileRelatedNames.contains(attr.name)) &&
    exec.output.forall {
      attr =>
        if (isPartitionColumn(attr)) isSupportedPartitionType(attr.dataType)
        else isSupportedDataType(attr.dataType)
    } &&
    relation.location.rootPaths.forall {
      path =>
        val scheme = path.toUri.getScheme
        scheme == null || scheme == "file"
    }
  }

  private def isSupportedDataType(dataType: DataType): Boolean = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
        StringType | BinaryType | DateType =>
      true
    case _: DecimalType => true
    case TimestampType => GlutenDataFusionConfig.get.scanTimestampEnabled
    case _ => false
  }

  /** Partition values arrive as strings and must be parseable natively. */
  private def isSupportedPartitionType(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType | StringType | DateType => true
    case _ => false
  }
}
