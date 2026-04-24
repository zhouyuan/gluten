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
package org.apache.gluten.datasource

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.exception.SchemaMismatchException
import org.apache.gluten.execution.RowToVeloxColumnarExec
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.memory.arrow.pool.ArrowNativeMemoryPool
import org.apache.gluten.utils.ArrowUtil
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, JoinedRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.json.{JSONOptions, JsonInferSchema}
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.json.JsonDataSource
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import org.apache.arrow.c.ArrowSchema
import org.apache.arrow.dataset.file.FileSystemDatasetFactory
import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.arrow.dataset.scanner.json.JsonFragmentScanOptions
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorUnloader
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import java.net.URLDecoder
import java.util.Optional

import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter}

class ArrowJSONFileFormat(parsedOptions: JSONOptions)
  extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {

  private val fileFormat = org.apache.arrow.dataset.file.FileFormat.JSON
  private lazy val pool = ArrowNativeMemoryPool.arrowPool("FileSystem Read")
  var fallback = false

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    false
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val arrowConfig = ArrowJSONOptionConverter.convert(parsedOptions)
    ArrowUtil.readSchema(
      files,
      fileFormat,
      arrowConfig,
      ArrowBufferAllocators.contextInstance(),
      ArrowNativeMemoryPool.arrowPool("infer schema"))
  }

  override def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = true

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val sqlConf = sparkSession.sessionState.conf
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val batchSize = sqlConf.columnBatchSize
    val columnPruning = sqlConf.jsonColumnPruning &&
      !requiredSchema.exists(_.name == sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    val actualFilters =
      filters.filterNot(_.references.contains(parsedOptions.columnNameOfCorruptRecord))
    (file: PartitionedFile) => {
      val actualDataSchema = StructType(
        dataSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
      val actualRequiredSchema = StructType(
        requiredSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))

      val arrowConfig = ArrowJSONOptionConverter.convert(parsedOptions)
      val allocator = ArrowBufferAllocators.contextInstance()
      
      // For JSON, we read all fields as Arrow doesn't support column pruning for JSON
      val requestSchema = actualRequiredSchema
      val missingSchema = new StructType()
      
      val cSchema: ArrowSchema = ArrowSchema.allocateNew(allocator)
      try {
        ArrowJSONOptionConverter.schema(requestSchema, cSchema, allocator, arrowConfig)
        val factory =
          ArrowUtil.makeArrowDiscovery(
            URLDecoder.decode(file.filePath.toString, "UTF-8"),
            fileFormat,
            Optional.of(arrowConfig),
            ArrowBufferAllocators.contextInstance(),
            pool)
        val fields = factory.inspect()
        ArrowJSONFileFormat
          .readArrow(
            ArrowBufferAllocators.contextInstance(),
            file,
            fields,
            missingSchema,
            partitionSchema,
            factory,
            batchSize,
            arrowConfig)
          .asInstanceOf[Iterator[InternalRow]]
      } catch {
        case e: SchemaMismatchException =>
          logWarning(e.getMessage)
          fallback = true
          val iter = ArrowJSONFileFormat.fallbackReadVanilla(
            dataSchema,
            requiredSchema,
            broadcastedHadoopConf.value.value,
            parsedOptions,
            file,
            actualFilters,
            columnPruning)
          val (schema, rows) =
            ArrowJSONFileFormat.withPartitionValue(requiredSchema, partitionSchema, iter, file)
          ArrowJSONFileFormat
            .rowToColumn(schema, batchSize, rows)
            .asInstanceOf[Iterator[InternalRow]]
        case d: Exception => throw d
      } finally {
        cSchema.close()
      }
    }
  }

  override def vectorTypes(
      requiredSchema: StructType,
      partitionSchema: StructType,
      sqlConf: SQLConf): Option[Seq[String]] = {
    Option(
      Seq.fill(requiredSchema.fields.length + partitionSchema.fields.length)(
        classOf[ArrowWritableColumnVector].getName
      ))
  }

  override def shortName(): String = "arrowjson"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[ArrowJSONFileFormat]

  override def prepareWrite(
      sparkSession: SparkSession,
      job: _root_.org.apache.hadoop.mapreduce.Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException()
  }
}

object ArrowJSONFileFormat {

  def readArrow(
      allocator: BufferAllocator,
      file: PartitionedFile,
      actualReadFields: Schema,
      missingSchema: StructType,
      partitionSchema: StructType,
      factory: FileSystemDatasetFactory,
      batchSize: Int,
      arrowConfig: JsonFragmentScanOptions): Iterator[ColumnarBatch] = {
    val actualReadFieldNames = actualReadFields.getFields.asScala.map(_.getName).toArray
    val dataset = factory.finish(actualReadFields)
    val scanOptions = new ScanOptions.Builder(batchSize)
      .columns(Optional.of(actualReadFieldNames))
      .fragmentScanOptions(arrowConfig)
      .build()
    val scanner = dataset.newScan(scanOptions)

    val partitionVectors =
      ArrowUtil.loadPartitionColumns(batchSize, partitionSchema, file.partitionValues)

    val nullVectors = if (missingSchema.nonEmpty) {
      ArrowUtil.loadMissingColumns(batchSize, missingSchema)
    } else {
      Array.empty[ArrowWritableColumnVector]
    }
    val reader = scanner.scanBatches()
    Iterators
      .wrap(new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = {
          reader.loadNextBatch()
        }

        override def next: ColumnarBatch = {
          val root = reader.getVectorSchemaRoot
          val unloader = new VectorUnloader(root)

          val batch = ArrowUtil.loadBatch(
            allocator,
            unloader.getRecordBatch,
            actualReadFields,
            partitionVectors,
            nullVectors)
          batch
        }
      })
      .recycleIterator {
        scanner.close()
        dataset.close()
        factory.close()
        reader.close()
        partitionVectors.foreach(_.close())
        nullVectors.foreach(_.close())
      }
      .recyclePayload(_.close())
      .create()
  }

  def rowToColumn(
      schema: StructType,
      batchSize: Int,
      it: Iterator[InternalRow]): Iterator[ColumnarBatch] = {
    val veloxBatch = RowToVeloxColumnarExec.toColumnarBatchIterator(
      it,
      schema,
      batchSize,
      VeloxConfig.get.veloxPreferredBatchBytes
    )
    veloxBatch
      .map(v => ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), v))
  }

  private def toAttribute(field: StructField): AttributeReference =
    AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()

  private def toAttributes(schema: StructType): Seq[AttributeReference] = {
    schema.map(toAttribute)
  }

  def withPartitionValue(
      requiredSchema: StructType,
      partitionSchema: StructType,
      iter: Iterator[InternalRow],
      file: PartitionedFile): (StructType, Iterator[InternalRow]) = {
    val fullSchema = toAttributes(requiredSchema) ++ toAttributes(partitionSchema)

    // Using lazy val to avoid serialization
    lazy val appendPartitionColumns =
      GenerateUnsafeProjection.generate(fullSchema, fullSchema)
    // Using local val to avoid per-row lazy val check (pre-mature optimization?...)
    val converter = appendPartitionColumns

    // Note that we have to apply the converter even though `file.partitionValues` is empty.
    // This is because the converter is also responsible for converting safe `InternalRow`s into
    // `UnsafeRow`s.
    if (partitionSchema.isEmpty) {
      val rows = iter.map(dataRow => converter(dataRow))
      (StructType(requiredSchema ++ partitionSchema), rows)
    } else {
      val joinedRow = new JoinedRow()
      val rows = iter.map(dataRow => converter(joinedRow(dataRow, file.partitionValues)))
      (StructType(requiredSchema ++ partitionSchema), rows)
    }
  }

  def fallbackReadVanilla(
      dataSchema: StructType,
      requiredSchema: StructType,
      conf: Configuration,
      parsedOptions: JSONOptions,
      file: PartitionedFile,
      actualFilters: Seq[Filter],
      columnPruning: Boolean): Iterator[InternalRow] = {
    val actualDataSchema = StructType(
      dataSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
    val actualRequiredSchema = StructType(
      requiredSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
    
    JsonDataSource(parsedOptions).readFile(
      conf,
      file,
      actualDataSchema,
      actualRequiredSchema,
      requiredSchema)
  }
}

// Made with Bob
