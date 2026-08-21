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

import io.substrait.proto._

/**
 * Assembles the self-contained, standard Substrait plan handed to DuckDB's substrait consumer
 * (from_substrait). The consumer resolves everything positionally:
 *
 *   - `ReadRel.local_files` becomes `parquet_scan([files])` outputting all file columns in
 *     physical order;
 *   - `ReadRel.projection` selects columns by position into that physical order (which is why the
 *     caller must first look the requested columns up in the file's physical schema, see
 *     [[org.apache.gluten.duckdb.DuckDBScanJniWrapper#describeParquet]]);
 *   - `RelRoot.names` renames the projected columns, again by position.
 */
object DuckDBSubstraitPlanBuilder {

  def build(
      baseSchema: NamedStruct,
      paths: Seq[String],
      projectionIndices: Seq[Int],
      names: Seq[String]): Array[Byte] = {
    require(projectionIndices.size == names.size, "one projection index per output column")
    val localFiles = ReadRel.LocalFiles.newBuilder()
    paths.foreach {
      path =>
        localFiles.addItems(
          ReadRel.LocalFiles.FileOrFiles
            .newBuilder()
            .setUriFile(path)
            .setParquet(ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions.getDefaultInstance))
    }
    val select = Expression.MaskExpression.StructSelect.newBuilder()
    projectionIndices.foreach {
      index =>
        select.addStructItems(Expression.MaskExpression.StructItem.newBuilder().setField(index))
    }
    val read = ReadRel
      .newBuilder()
      .setBaseSchema(baseSchema)
      .setLocalFiles(localFiles)
      .setProjection(Expression.MaskExpression.newBuilder().setSelect(select))
    val root = RelRoot.newBuilder().setInput(Rel.newBuilder().setRead(read))
    names.foreach(root.addNames)
    Plan
      .newBuilder()
      .setVersion(Version.newBuilder().setMinorNumber(53).setProducer("gluten-duckdb"))
      .addRelations(PlanRel.newBuilder().setRoot(root))
      .build()
      .toByteArray
  }
}
