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

import org.apache.gluten.utils.ArrowAbiUtil

import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkSchemaUtil

import com.google.common.collect.ImmutableMap
import org.apache.arrow.c.ArrowSchema
import org.apache.arrow.dataset.scanner.json.{JsonFragmentScanOptions, JsonParseOptions}
import org.apache.arrow.memory.BufferAllocator

import java.util

object ArrowJSONOptionConverter {
  def convert(option: JSONOptions): JsonFragmentScanOptions = {
    val parseMap = new util.HashMap[String, String]()
    
    // Set default options for JSON parsing
    parseMap.put("unexpected_field_behavior", "infer")
    
    val parseOptions = new JsonParseOptions(parseMap)
    new JsonFragmentScanOptions(parseOptions)
  }

  def schema(
      requiredSchema: StructType,
      cSchema: ArrowSchema,
      allocator: BufferAllocator,
      option: JsonFragmentScanOptions): Unit = {
    val schema = SparkSchemaUtil.toArrowSchema(requiredSchema)
    ArrowAbiUtil.exportSchema(allocator, schema, cSchema)
    option.getParseOptions.setExplicitSchema(cSchema)
  }
}

// Made with Bob
