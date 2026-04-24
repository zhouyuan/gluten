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
package org.apache.gluten.datasource.v2

import org.apache.gluten.datasource.{ArrowJSONFileFormat, ArrowJSONOptionConverter}
import org.apache.gluten.exception.SchemaMismatchException
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.memory.arrow.pool.ArrowNativeMemoryPool
import org.apache.gluten.utils.ArrowUtil

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources
import org.apache.spark.util.SerializableConfiguration

import org.apache.arrow.c.ArrowSchema

import java.net.URLDecoder
import java.util.Optional

case class ArrowJSONPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: JSONOptions,
    filters: Seq[Filter])
  extends FilePartitionReaderFactory
  with Logging {

  private val batchSize = sqlConf.parquetVectorizedReaderBatchSize
  private val jsonColumnPruning: Boolean = sqlConf.jsonColumnPruning
  private val fileFormat = org.apache.arrow.dataset.file.FileFormat.JSON
  var fallback = false

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    // disable row based read
    throw new UnsupportedOperationException
  }

  override def buildColumnarReader(
      partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val actualDataSchema = StructType(
      dataSchema.filterNot(_.name == options.columnNameOfCorruptRecord))
    val actualRequiredSchema = StructType(
      readDataSchema.filterNot(_.name == options.columnNameOfCorruptRecord))
    
    val (allocator, pool) = if (!TaskResources.inSparkTask()) {
      TaskResources.runUnsafe(
        (
          ArrowBufferAllocators.contextInstance(),
          ArrowNativeMemoryPool.arrowPool("FileSystemFactory"))
      )
    } else {
      (
        ArrowBufferAllocators.contextInstance(),
        ArrowNativeMemoryPool.arrowPool("FileSystemFactory"))
    }
    val arrowConfig = ArrowJSONOptionConverter.convert(options)
    
    // For JSON, we read the required schema directly
    val requestSchema = actualRequiredSchema
    val missingSchema = new StructType()
    
    val cSchema: ArrowSchema = ArrowSchema.allocateNew(allocator)
    val iter =
      try {
        ArrowJSONOptionConverter.schema(requestSchema, cSchema, allocator, arrowConfig)
        val factory =
          ArrowUtil.makeArrowDiscovery(
            URLDecoder.decode(partitionedFile.filePath.toString, "UTF-8"),
            fileFormat,
            Optional.of(arrowConfig),
            ArrowBufferAllocators.contextInstance(),
            pool)
        val fields = factory.inspect()
        ArrowJSONFileFormat
          .readArrow(
            ArrowBufferAllocators.contextInstance(),
            partitionedFile,
            fields,
            missingSchema,
            readPartitionSchema,
            factory,
            batchSize,
            arrowConfig)
      } catch {
        case e: SchemaMismatchException =>
          logWarning(e.getMessage)
          fallback = true
          val iter = ArrowJSONFileFormat.fallbackReadVanilla(
            dataSchema,
            readDataSchema,
            broadcastedConf.value.value,
            options,
            partitionedFile,
            filters,
            jsonColumnPruning)
          val (schema, rows) = ArrowJSONFileFormat.withPartitionValue(
            readDataSchema,
            readPartitionSchema,
            iter,
            partitionedFile)
          ArrowJSONFileFormat.rowToColumn(schema, batchSize, rows)
        case d: Exception => throw d
      } finally {
        cSchema.close()
      }

    new PartitionReader[ColumnarBatch] {

      override def next(): Boolean = {
        iter.hasNext
      }

      override def get(): ColumnarBatch = {
        iter.next()
      }

      override def close(): Unit = {}
    }
  }

}

// Made with Bob
