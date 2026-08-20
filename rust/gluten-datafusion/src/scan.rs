// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Builds and executes a DataFusion parquet scan for a parsed `ScanSpec`.

use std::sync::Arc;

use datafusion::arrow::datatypes::{FieldRef, Schema};
use datafusion::common::config::TableParquetOptions;
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr, SendableRecordBatchStream};
use datafusion::prelude::SessionConfig;

use crate::error::{GdfError, Result};
use crate::plan::{ScanFile, ScanSpec, HIVE_DEFAULT_PARTITION};

#[derive(Debug, Clone)]
pub struct ScanOptions {
    pub batch_size: usize,
}

impl Default for ScanOptions {
    fn default() -> Self {
        ScanOptions { batch_size: 4096 }
    }
}

/// Turns a `ScanSpec` into an executing DataFusion record-batch stream whose
/// schema matches the spec's column order and names exactly.
pub fn create_stream(spec: &ScanSpec, options: &ScanOptions) -> Result<SendableRecordBatchStream> {
    let file_fields: Vec<_> = spec
        .columns
        .iter()
        .filter(|c| !c.is_partition)
        .map(|c| c.field.clone())
        .collect();
    let partition_fields: Vec<FieldRef> = spec
        .columns
        .iter()
        .filter(|c| c.is_partition)
        .map(|c| Arc::new(c.field.clone()))
        .collect();
    if file_fields.is_empty() {
        return Err(GdfError::new(
            "scan with no data columns is not supported by stage-1 scan offload",
        ));
    }
    let file_schema = Arc::new(Schema::new(file_fields));

    let files = spec
        .files
        .iter()
        .map(|f| to_partitioned_file(f, &partition_fields))
        .collect::<Result<Vec<_>>>()?;

    let config = SessionConfig::new().with_batch_size(options.batch_size);
    let ctx = SessionContext::new_with_config(config);

    let mut parquet_options = TableParquetOptions::default();
    // Keep plain Utf8/Binary in the output: the batches are handed to Velox
    // through the Arrow C Data Interface and view types would complicate the
    // import.
    parquet_options.global.schema_force_view_types = false;
    parquet_options.global.pushdown_filters = false;

    let table_schema = TableSchema::builder(file_schema)
        .with_table_partition_cols(partition_fields.clone())
        .build();
    let source = ParquetSource::new(table_schema).with_table_parquet_options(parquet_options);
    let scan_config = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        Arc::new(source),
    )
    .with_file_group(FileGroup::new(files))
    .with_batch_size(Some(options.batch_size))
    .build();
    let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(scan_config);

    // The scan outputs file columns first and partition columns appended at
    // the end; restore the exact output order the downstream stage expects.
    let scan_schema = scan.schema();
    let projection = spec
        .columns
        .iter()
        .map(|c| {
            let name = c.field.name();
            let index = scan_schema.index_of(name)?;
            Ok((
                Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>,
                name.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let exec = Arc::new(ProjectionExec::try_new(projection, scan)?);

    Ok(exec.execute(0, ctx.task_ctx())?)
}

fn to_partitioned_file(file: &ScanFile, partition_fields: &[FieldRef]) -> Result<PartitionedFile> {
    let partition_values = partition_fields
        .iter()
        .map(|field| {
            // Split-info keys carry the partition schema's original casing
            // while base_schema names may be case-normalized; match loosely.
            let value = file
                .partition_values
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(field.name()))
                .map(|(_, v)| v)
                .ok_or_else(|| {
                    GdfError::new(format!(
                        "no partition value for column '{}' in file '{}'",
                        field.name(),
                        file.path
                    ))
                })?;
            if value == HIVE_DEFAULT_PARTITION {
                Ok(ScalarValue::try_from(field.data_type())?)
            } else {
                Ok(ScalarValue::try_from_string(
                    value.clone(),
                    field.data_type(),
                )?)
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let mut partitioned = PartitionedFile::new(file.path.clone(), file.file_size);
    if !(file.start == 0 && file.length == file.file_size) {
        partitioned = partitioned.with_range(file.start as i64, (file.start + file.length) as i64);
    }
    partitioned.partition_values = partition_values;
    Ok(partitioned)
}
