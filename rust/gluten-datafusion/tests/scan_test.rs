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

//! End-to-end tests: substrait ReadRel + LocalFiles -> DataFusion scan ->
//! record batches, over real parquet files.

use std::collections::HashSet;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use futures::StreamExt;
use prost::Message;

use gluten_datafusion::plan::{self, HIVE_DEFAULT_PARTITION};
use gluten_datafusion::proto::substrait as sb;
use gluten_datafusion::proto::substrait::r#type::{self as sbt, Kind, Nullability};
use gluten_datafusion::proto::substrait::read_rel::local_files::file_or_files;
use gluten_datafusion::proto::substrait::read_rel::local_files::file_or_files::{
    FileFormat, PathType,
};
use gluten_datafusion::proto::substrait::rel::RelType;
use gluten_datafusion::{runtime, scan};

fn i64_type() -> sb::Type {
    sb::Type {
        kind: Some(Kind::I64(sbt::I64 {
            type_variation_reference: 0,
            nullability: Nullability::Nullable as i32,
        })),
    }
}

fn string_type() -> sb::Type {
    sb::Type {
        kind: Some(Kind::String(sbt::String {
            type_variation_reference: 0,
            nullability: Nullability::Nullable as i32,
        })),
    }
}

fn plan_bytes(names: Vec<&str>, types: Vec<sb::Type>, column_types: Vec<i32>) -> Vec<u8> {
    let read = sb::ReadRel {
        base_schema: Some(sb::NamedStruct {
            names: names.into_iter().map(|s| s.to_string()).collect(),
            r#struct: Some(sbt::Struct {
                types,
                type_variation_reference: 0,
                nullability: Nullability::Required as i32,
                names: vec![],
            }),
            column_types,
        }),
        ..Default::default()
    };
    sb::Plan {
        relations: vec![sb::PlanRel {
            rel_type: Some(sb::plan_rel::RelType::Root(sb::RelRoot {
                input: Some(sb::Rel {
                    rel_type: Some(RelType::Read(Box::new(read))),
                }),
                names: vec![],
                output_schema: None,
            })),
        }],
        ..Default::default()
    }
    .encode_to_vec()
}

struct SplitFile<'a> {
    path: &'a str,
    start: u64,
    length: u64,
    file_size: u64,
    partition_values: Vec<(&'a str, &'a str)>,
}

fn split_bytes(files: &[SplitFile<'_>]) -> Vec<u8> {
    sb::read_rel::LocalFiles {
        items: files
            .iter()
            .map(|f| sb::read_rel::local_files::FileOrFiles {
                path_type: Some(PathType::UriFile(format!("file://{}", f.path))),
                file_format: Some(FileFormat::Parquet(file_or_files::ParquetReadOptions {})),
                start: f.start,
                length: f.length,
                properties: Some(file_or_files::FileProperties {
                    file_size: f.file_size as i64,
                    modification_time: 0,
                }),
                partition_columns: f
                    .partition_values
                    .iter()
                    .map(|(k, v)| file_or_files::PartitionColumn {
                        key: k.to_string(),
                        value: v.to_string(),
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        advanced_extension: None,
    }
    .encode_to_vec()
}

/// Writes a parquet file of (id: int64, name: utf8) rows with the given row
/// group size and returns its size in bytes.
fn write_parquet(path: &std::path::Path, num_rows: i64, row_group_size: usize) -> u64 {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let ids = Int64Array::from_iter_values(0..num_rows);
    let names = StringArray::from_iter_values((0..num_rows).map(|i| format!("row-{i}")));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)]).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(row_group_size)
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    std::fs::metadata(path).unwrap().len()
}

fn run_scan(plan: &[u8], split: &[u8], batch_size: usize) -> Vec<RecordBatch> {
    let spec = plan::parse(plan, split).unwrap();
    let rt = runtime::global(0);
    let _guard = rt.enter();
    let mut stream = scan::create_stream(&spec, &scan::ScanOptions { batch_size }).unwrap();
    let mut batches = vec![];
    while let Some(batch) = rt.block_on(stream.next()) {
        batches.push(batch.unwrap());
    }
    batches
}

fn collect_ids(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn reads_full_file_with_projection_order() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("data.parquet");
    let file_size = write_parquet(&file, 100, 1000);

    // Project (name, id): reversed relative to the file's column order.
    let plan = plan_bytes(
        vec!["name", "id"],
        vec![string_type(), i64_type()],
        vec![0, 0],
    );
    let split = split_bytes(&[SplitFile {
        path: file.to_str().unwrap(),
        start: 0,
        length: file_size,
        file_size,
        partition_values: vec![],
    }]);

    let batches = run_scan(&plan, &split, 4096);
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 100);
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "name");
    assert_eq!(schema.field(1).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    let ids = collect_ids(&batches, 1);
    assert_eq!(ids, (0..100).collect::<Vec<_>>());
}

#[test]
fn byte_range_splits_read_each_row_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("split.parquet");
    // Many small row groups so that a mid-file split boundary is meaningful.
    let file_size = write_parquet(&file, 1000, 50);

    let plan = plan_bytes(vec!["id"], vec![i64_type()], vec![0]);
    let mid = file_size / 2;
    let splits = [(0, mid), (mid, file_size - mid)]
        .iter()
        .map(|&(start, length)| {
            split_bytes(&[SplitFile {
                path: file.to_str().unwrap(),
                start,
                length,
                file_size,
                partition_values: vec![],
            }])
        })
        .collect::<Vec<_>>();

    let mut all_ids = vec![];
    for split in &splits {
        all_ids.extend(collect_ids(&run_scan(&plan, split, 128), 0));
    }
    assert_eq!(all_ids.len(), 1000, "rows dropped or duplicated across splits");
    assert_eq!(all_ids.iter().collect::<HashSet<_>>().len(), 1000);
}

#[test]
fn synthesizes_partition_columns_including_null() {
    let dir = tempfile::tempdir().unwrap();
    let file_a = dir.path().join("a.parquet");
    let file_b = dir.path().join("b.parquet");
    let size_a = write_parquet(&file_a, 10, 1000);
    let size_b = write_parquet(&file_b, 10, 1000);

    // Output order interleaves a partition column: (id, p_date, name).
    let plan = plan_bytes(
        vec!["id", "p_date", "name"],
        vec![i64_type(), string_type(), string_type()],
        vec![0, 1, 0],
    );
    let split = split_bytes(&[
        SplitFile {
            path: file_a.to_str().unwrap(),
            start: 0,
            length: size_a,
            file_size: size_a,
            partition_values: vec![("p_date", "2024-01-01")],
        },
        SplitFile {
            path: file_b.to_str().unwrap(),
            start: 0,
            length: size_b,
            file_size: size_b,
            partition_values: vec![("p_date", HIVE_DEFAULT_PARTITION)],
        },
    ]);

    let batches = run_scan(&plan, &split, 4096);
    let schema = batches[0].schema();
    assert_eq!(schema.field(1).name(), "p_date");
    let mut values = vec![];
    for batch in &batches {
        let col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..col.len() {
            values.push(if col.is_null(i) {
                None
            } else {
                Some(col.value(i).to_string())
            });
        }
    }
    assert_eq!(values.len(), 20);
    assert_eq!(
        values.iter().filter(|v| v.as_deref() == Some("2024-01-01")).count(),
        10
    );
    assert_eq!(values.iter().filter(|v| v.is_none()).count(), 10);
}

#[test]
fn missing_file_errors_cleanly() {
    let plan = plan_bytes(vec!["id"], vec![i64_type()], vec![0]);
    let split = split_bytes(&[SplitFile {
        path: "/definitely/not/a/real/file.parquet",
        start: 0,
        length: 10,
        file_size: 10,
        partition_values: vec![],
    }]);
    let spec = plan::parse(&plan, &split).unwrap();
    let rt = runtime::global(0);
    let _guard = rt.enter();
    let mut stream = scan::create_stream(&spec, &scan::ScanOptions::default()).unwrap();
    let first = rt.block_on(stream.next());
    assert!(matches!(first, Some(Err(_))), "expected an IO error");
}
