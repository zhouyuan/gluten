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
package org.apache.spark.sql.execution

import org.apache.gluten.metrics.GlutenTimeMetric
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

import java.util.concurrent.TimeUnit._

abstract class FileSourceScanExecShim(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false)
  extends AbstractFileSourceScanExec(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan) {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] = Map()

  lazy val metadataColumns: Seq[AttributeReference] = output.collect {
    case FileSourceConstantMetadataAttribute(attr) => attr
    case FileSourceGeneratedMetadataAttribute(attr, _) => attr
  }

  protected lazy val driverMetricsAlias = driverMetrics

  def dataFiltersInScan: Seq[Expression] = dataFilters.filterNot(_.references.exists {
    attr => SparkShimLoader.getSparkShims.isRowIndexMetadataColumn(attr.name)
  })

  def hasUnsupportedColumns: Boolean = {
    // TODO, fallback if user define same name column due to we can't right now
    // detect which column is metadata column which is user defined column.
    val metadataColumnsNames = metadataColumns.map(_.name)
    output
      .filterNot(metadataColumns.toSet)
      .exists(v => metadataColumnsNames.contains(v.name))
  }

  def isMetadataColumn(attr: Attribute): Boolean = metadataColumns.contains(attr)

  def hasFieldIds: Boolean = ParquetUtils.hasFieldIds(requiredSchema)

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

  protected def setFilesNumAndSizeMetric(partitions: ScanFileListing, static: Boolean): Unit = {
    val filesNum = partitions.totalNumberOfFiles
    val filesSize = partitions.totalFileSize
    if (!static || !partitionFilters.exists(isDynamicPruningFilter)) {
      driverMetrics("numFiles").set(filesNum)
      driverMetrics("filesSize").set(filesSize)
    } else {
      driverMetrics("staticFilesNum").set(filesNum)
      driverMetrics("staticFilesSize").set(filesSize)
    }
    if (relation.partitionSchema.nonEmpty) {
      driverMetrics("numPartitions").set(partitions.partitionCount)
    }
  }

  // This field will be accessed during planning (e.g., `outputPartitioning` relies on it), and can
  // only use static filters.
  @transient lazy val selectedPartitions: ScanFileListing = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    // The filters may contain subquery expressions which can't be evaluated during planning.
    // Here we filter out subquery expressions and get the static data/partition filters, so that
    // they can be used to do pruning at the planning phase.
    val staticDataFilters = dataFilters.filterNot(isDynamicPruningFilter)
    val staticPartitionFilters = partitionFilters.filterNot(isDynamicPruningFilter)
    val partitionDirectories =
      relation.location.listFiles(staticPartitionFilters, staticDataFilters)
    val fileListing = GenericScanFileListing(partitionDirectories.toArray)
    setFilesNumAndSizeMetric(fileListing, static = true)
    val timeTakenMs =
      NANOSECONDS.toMillis((System.nanoTime() - startTime) + optimizerMetadataTimeNs)
    driverMetrics("metadataTime").set(timeTakenMs)
    fileListing
  }

  @transient override lazy val dynamicallySelectedPartitions: ScanFileListing = {
    val dynamicDataFilters = dataFilters.filter(isDynamicPruningFilter)
    val dynamicPartitionFilters =
      partitionFilters.filter(isDynamicPruningFilter)
    if (dynamicPartitionFilters.nonEmpty) {
      GlutenTimeMetric.withMillisTime {
        // call the file index for the files matching all filters except dynamic partition filters
        val predicate = dynamicPartitionFilters.reduce(And)
        val partitionColumns = relation.partitionSchema
        val boundPredicate = Predicate.create(
          predicate.transform {
            case a: AttributeReference =>
              val index = partitionColumns.indexWhere(a.name == _.name)
              BoundReference(index, partitionColumns(index).dataType, nullable = true)
          },
          Nil
        )
        val returnedFiles =
          selectedPartitions.filterAndPruneFiles(boundPredicate, dynamicDataFilters)
        setFilesNumAndSizeMetric(returnedFiles, false)
        returnedFiles
      }(t => driverMetrics("pruningTime").set(t))
    } else {
      selectedPartitions
    }
  }

  private case class GenericScanFileListing(partitionDirectories: Array[PartitionDirectory])
    extends ScanFileListing {

    override def partitionCount: Int = partitionDirectories.length

    override def totalFileSize: Long = partitionDirectories.map(_.files.map(_.getLen).sum).sum

    override def totalNumberOfFiles: Long = partitionDirectories.map(_.files.length).sum.toLong

    override def filterAndPruneFiles(
        boundPredicate: BasePredicate,
        dynamicFileFilters: Seq[Expression]): ScanFileListing = {
      val filteredPartitions = partitionDirectories.filter(p => boundPredicate.eval(p.values))
      val prunedPartitions = if (dynamicFileFilters.nonEmpty) {
        val filePruningRunner = new FilePruningRunner(dynamicFileFilters)
        filteredPartitions.map(filePruningRunner.prune)
      } else {
        filteredPartitions
      }
      GenericScanFileListing(prunedPartitions)
    }

    override def toPartitionArray: Array[PartitionedFile] = {
      partitionDirectories.flatMap {
        p => p.files.map(f => PartitionedFileUtil.getPartitionedFile(f, p.values, 0, f.getLen))
      }
    }

    override def calculateTotalPartitionBytes: Long = {
      val openCostInBytes = relation.sparkSession.sessionState.conf.filesOpenCostInBytes
      partitionDirectories.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    }

    override def filePartitionIterator: Iterator[ListingPartition] = {
      partitionDirectories.iterator.map {
        partitionDirectory =>
          ListingPartition(
            partitionDirectory.values,
            partitionDirectory.files.size,
            partitionDirectory.files.iterator)
      }
    }

    override def bucketsContainSingleFile: Boolean = {
      val files = partitionDirectories.flatMap(_.files)
      val bucketToFilesGrouping =
        files.map(_.getPath.getName).groupBy(file => BucketingUtils.getBucketId(file))
      bucketToFilesGrouping.forall(p => p._2.length <= 1)
    }
  }
}

abstract class ArrowFileSourceScanLikeShim(original: FileSourceScanExec)
  extends FileSourceScanLike {
  override val nodeNamePrefix: String = "ArrowFile"

  override def tableIdentifier: Option[TableIdentifier] = original.tableIdentifier

  override def inputRDDs(): Seq[RDD[InternalRow]] = original.inputRDDs()

  override def dataFilters: Seq[Expression] = original.dataFilters

  override def disableBucketedScan: Boolean = original.disableBucketedScan

  override def optionalBucketSet: Option[BitSet] = original.optionalBucketSet

  override def optionalNumCoalescedBuckets: Option[Int] = original.optionalNumCoalescedBuckets

  override def partitionFilters: Seq[Expression] = original.partitionFilters

  override def relation: HadoopFsRelation = original.relation

  override def requiredSchema: StructType = original.requiredSchema
}
