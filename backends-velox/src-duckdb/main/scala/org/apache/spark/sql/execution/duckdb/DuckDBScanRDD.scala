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
package org.apache.spark.sql.execution.duckdb

import org.apache.gluten.columnarbatch.{ColumnarBatches, ColumnarBatchJniWrapper}
import org.apache.gluten.duckdb.DuckDBScanJniWrapper
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.execution.DuckDBSubstraitPlanBuilder
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.arrow.c.{ArrowArray, ArrowSchema}

import io.substrait.proto.NamedStruct

import scala.collection.JavaConverters._

case class DuckDBScanSplit(paths: Array[String], locations: Array[String])

private case class DuckDBScanPartition(index: Int, split: DuckDBScanSplit) extends Partition

/**
 * Runs one DuckDB scan per partition and exposes its Arrow output as native-handle-backed
 * ("light") ColumnarBatches, ready to be consumed by a downstream Velox stage.
 *
 * The per-task Substrait plan is assembled here rather than on the driver: DuckDB's substrait
 * consumer selects columns by position in the parquet file's physical schema, which is only
 * discovered (via describeParquet) next to the data.
 */
class DuckDBScanRDD(
    @transient sc: SparkContext,
    namedStructBytes: Array[Byte],
    splits: Seq[DuckDBScanSplit],
    threads: Int,
    memoryLimit: String,
    substraitExtensionPath: String,
    numOutputRows: SQLMetric,
    numOutputBatches: SQLMetric,
    scanTime: SQLMetric)
  extends RDD[ColumnarBatch](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    splits.zipWithIndex.map { case (split, i) => DuckDBScanPartition(i, split) }.toArray
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DuckDBScanPartition].split.locations
  }

  /**
   * Positions of the requested columns in the file's physical column order. All files of a split
   * are written by the same job and share one physical layout, so the first file speaks for the
   * split (from_substrait's parquet_scan requires identical schemas anyway).
   */
  private def projectionIndices(names: Seq[String], paths: Array[String]): Seq[Int] = {
    val fileColumns = DuckDBScanJniWrapper.describeParquet(paths.head)
    names.map {
      name =>
        val matches = fileColumns.zipWithIndex.filter(_._1.equalsIgnoreCase(name))
        matches match {
          case Array((_, index)) => index
          case Array() =>
            throw new GlutenException(
              s"Column '$name' not found in parquet file '${paths.head}' " +
                s"(file columns: ${fileColumns.mkString(", ")})")
          case _ =>
            throw new GlutenException(
              s"Column '$name' is ambiguous in parquet file '${paths.head}' " +
                s"(file columns: ${fileColumns.mkString(", ")})")
        }
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partition = split.asInstanceOf[DuckDBScanPartition]
    if (partition.split.paths.isEmpty) {
      return Iterator.empty
    }
    val namedStruct = NamedStruct.parseFrom(namedStructBytes)
    val names = namedStruct.getNamesList.asScala.toSeq
    val planBytes = DuckDBSubstraitPlanBuilder.build(
      namedStruct,
      partition.split.paths,
      projectionIndices(names, partition.split.paths),
      names)
    val handle =
      DuckDBScanJniWrapper.open(planBytes, threads, memoryLimit, substraitExtensionPath)
    // The "internal" runtime only stores native batch references; no backend-specific
    // native code is involved (same choice as ColumnarBatches#offload).
    val runtime = Runtimes.contextInstance("internal", "DuckDBScanRDD")
    val batchJniWrapper = ColumnarBatchJniWrapper.create(runtime)
    val allocator = ArrowBufferAllocators.contextInstance()

    var pending: ColumnarBatch = null
    val iterator = new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = {
        if (pending != null) {
          return true
        }
        // The shells only carry the C structs across the boundary: the native side
        // moves the exported schema/array into them and createWithArrowArray
        // immediately moves them out into a native batch.
        val cSchema = ArrowSchema.allocateNew(allocator)
        val cArray = ArrowArray.allocateNew(allocator)
        try {
          if (DuckDBScanJniWrapper.next(handle, cSchema.memoryAddress(), cArray.memoryAddress())) {
            val batchHandle =
              batchJniWrapper.createWithArrowArray(cSchema.memoryAddress(), cArray.memoryAddress())
            pending = ColumnarBatches.create(batchHandle)
            true
          } else {
            false
          }
        } finally {
          cSchema.close()
          cArray.close()
        }
      }

      override def next(): ColumnarBatch = {
        if (!hasNext) {
          throw new NoSuchElementException("End of DuckDB scan stream")
        }
        val batch = pending
        pending = null
        numOutputRows += batch.numRows()
        numOutputBatches += 1
        batch
      }
    }

    Iterators
      .wrap(iterator)
      .protectInvocationFlow()
      .collectReadMillis(scanTime += _)
      .recycleIterator {
        if (pending != null) {
          pending.close()
          pending = null
        }
        DuckDBScanJniWrapper.close(handle)
      }
      .recyclePayload(_.close())
      .create()
  }
}
