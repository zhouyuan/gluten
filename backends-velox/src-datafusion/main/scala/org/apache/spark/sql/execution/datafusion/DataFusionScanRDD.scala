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
package org.apache.spark.sql.execution.datafusion

import org.apache.gluten.columnarbatch.{ColumnarBatches, ColumnarBatchJniWrapper}
import org.apache.gluten.datafusion.DataFusionScanJniWrapper
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.arrow.c.{ArrowArray, ArrowSchema}

case class DataFusionScanSplit(splitBytes: Array[Byte], locations: Array[String])

private case class DataFusionScanPartition(index: Int, split: DataFusionScanSplit)
  extends Partition

/**
 * Runs one DataFusion scan per partition and exposes its Arrow output as native-handle-backed
 * ("light") ColumnarBatches, ready to be consumed by a downstream Velox stage.
 */
class DataFusionScanRDD(
    @transient sc: SparkContext,
    planBytes: Array[Byte],
    splits: Seq[DataFusionScanSplit],
    confJson: Array[Byte],
    numOutputRows: SQLMetric,
    numOutputBatches: SQLMetric,
    scanTime: SQLMetric)
  extends RDD[ColumnarBatch](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    splits.zipWithIndex.map { case (split, i) => DataFusionScanPartition(i, split) }.toArray
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataFusionScanPartition].split.locations
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partition = split.asInstanceOf[DataFusionScanPartition]
    val handle = DataFusionScanJniWrapper.open(planBytes, partition.split.splitBytes, confJson)
    // The "internal" runtime only stores native batch references; no backend-specific
    // native code is involved (same choice as ColumnarBatches#offload).
    val runtime = Runtimes.contextInstance("internal", "DataFusionScanRDD")
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
          if (
            DataFusionScanJniWrapper.next(handle, cSchema.memoryAddress(), cArray.memoryAddress())
          ) {
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
          throw new NoSuchElementException("End of DataFusion scan stream")
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
        DataFusionScanJniWrapper.close(handle)
      }
      .recyclePayload(_.close())
      .create()
  }
}
