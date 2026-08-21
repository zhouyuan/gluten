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

import org.apache.gluten.backendsapi.arrow.ArrowBatchTypes
import org.apache.gluten.component.VeloxDuckDBComponent
import org.apache.gluten.config.{GlutenConfig, GlutenDuckDBConfig}
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.{FileSourceScanExec, FileSourceScanExecShim}
import org.apache.spark.sql.execution.datasources.{FilePartition, HadoopFsRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.duckdb.{DuckDBScanRDD, DuckDBScanSplit}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.collection.BitSet

import org.apache.hadoop.fs.Path

import io.substrait.proto.{NamedStruct, Type}

import scala.collection.JavaConverters._

/**
 * A parquet table scan executed by DuckDB through its substrait extension, producing Arrow-native
 * batches. It runs as its own leaf stage: downstream Velox operators consume its output through the
 * regular InputIteratorTransformer / value-stream mechanism, with the Arrow-to-Velox transition
 * inserted automatically by the transition planner.
 */
case class DuckDBScanExec(
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
    s"DuckDBScan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"

  override val nodeNamePrefix: String = ""

  /**
   * Packs the (dynamically pruned) input files into partitions of whole files. DuckDB's
   * parquet_scan cannot read a byte range of a file, so unlike the regular scan path a file is
   * never split; files larger than maxSplitBytes simply make bigger tasks.
   */
  private def genWholeFilePartitions(): Seq[FilePartition] = {
    val selectedPartitions = getPartitionArray
    val maxSplitBytes =
      FilePartition.maxSplitBytes(relation.sparkSession, selectedPartitions)
    val wholeFiles = selectedPartitions
      .flatMap {
        partition =>
          SparkShimLoader.getSparkShims.getFileStatus(partition).flatMap {
            file =>
              SparkShimLoader.getSparkShims.splitFiles(
                sparkSession = relation.sparkSession,
                file = file._1,
                filePath = file._1.getPath,
                isSplitable = false,
                maxSplitBytes = maxSplitBytes,
                partitionValues = partition.values,
                metadata = file._2
              )
          }
      }
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    FilePartition.getFilePartitions(relation.sparkSession, wholeFiles, maxSplitBytes)
  }

  /**
   * Serializes the requested output columns (names and Substrait types, in output order) for the
   * per-task Substrait plan assembly in [[DuckDBScanRDD]].
   */
  private def buildNamedStructBytes(): Array[Byte] = {
    val nameList = ConverterUtils.collectAttributeNamesWithoutExprId(output)
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(output)
    val structBuilder = Type.Struct.newBuilder()
    typeNodes.asScala.foreach(node => structBuilder.addTypes(node.toProtobuf))
    NamedStruct
      .newBuilder()
      .addAllNames(nameList)
      .setStruct(structBuilder)
      .build()
      .toByteArray
  }

  /** Turns the (possibly URI-encoded) PartitionedFile path into a local filesystem path. */
  private def toLocalPath(uriString: String): String = {
    val path = Option(new Path(uriString).toUri.getPath).filter(_.nonEmpty)
    path.getOrElse(
      throw new GlutenException(s"Cannot convert '$uriString' to a local filesystem path"))
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val namedStructBytes = buildNamedStructBytes()
    val splits = genWholeFilePartitions().map {
      partition =>
        DuckDBScanSplit(
          partition.files.map(f => toLocalPath(f.filePath.toString)),
          partition.preferredLocations())
    }
    val conf = GlutenDuckDBConfig.get
    new DuckDBScanRDD(
      sparkContext,
      namedStructBytes,
      splits,
      conf.threads,
      conf.memoryLimit,
      conf.substraitExtensionPath,
      longMetric("numOutputRows"),
      longMetric("numOutputBatches"),
      longMetric("scanTime")
    )
  }

  override def doCanonicalize(): DuckDBScanExec = {
    DuckDBScanExec(
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

object DuckDBScanExec {

  /**
   * Offloads the scan to DuckDB when supported; returns None to leave it to the regular (Velox)
   * scan path.
   */
  def tryOffload(scan: FileSourceScanExec): Option[DuckDBScanExec] = {
    if (!GlutenDuckDBConfig.get.scanEnabled || !GlutenConfig.get.enableColumnarFileScan) {
      return None
    }
    if (!VeloxDuckDBComponent.nativeLibLoaded) {
      return None
    }
    if (!scan.relation.fileFormat.isInstanceOf[ParquetFileFormat]) {
      return None
    }
    val exec = DuckDBScanExec(
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

  private def supports(exec: DuckDBScanExec): Boolean = {
    val relation = exec.relation
    def isPartitionColumn(attr: Attribute): Boolean =
      relation.partitionSchema.exists(_.name.equals(attr.name))
    // Stage-1 restrictions; anything outside falls back to the Velox scan. Notably, DuckDB's
    // substrait consumer cannot synthesize Hive partition column values, so any scan reading a
    // partition column stays on Velox (reading data columns of a partitioned table is fine).
    exec.output.nonEmpty &&
    exec.output.forall(!isPartitionColumn(_)) &&
    !exec.bucketedScan &&
    exec.metadataColumns.isEmpty &&
    !exec.hasFieldIds &&
    exec.output.forall(attr => !inputFileRelatedNames.contains(attr.name)) &&
    exec.output.forall(attr => isSupportedDataType(attr.dataType)) &&
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
    case TimestampType => GlutenDuckDBConfig.get.scanTimestampEnabled
    case _ => false
  }
}
