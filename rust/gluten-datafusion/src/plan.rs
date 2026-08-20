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

//! Parses the Substrait plan (single ReadRel) and the per-task LocalFiles
//! split that Gluten's JVM side serializes, into a self-contained `ScanSpec`.

use std::collections::HashMap;

use prost::Message;

use crate::error::{GdfError, Result};
use crate::proto::substrait as sb;
use crate::proto::substrait::read_rel::local_files::file_or_files::{FileFormat, PathType};
use crate::proto::substrait::rel::RelType;
use crate::types::to_arrow_field;

/// Spark's placeholder for a null partition value, see
/// `ExternalCatalogUtils.DEFAULT_PARTITION_NAME`.
pub const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

#[derive(Debug, Clone)]
pub struct ScanColumn {
    pub field: datafusion::arrow::datatypes::Field,
    pub is_partition: bool,
}

#[derive(Debug, Clone)]
pub struct ScanFile {
    /// Absolute local filesystem path (URI-decoded).
    pub path: String,
    pub start: u64,
    pub length: u64,
    pub file_size: u64,
    /// Partition column name -> string value, as emitted by
    /// `VeloxIteratorApi.getPartitionColumns`.
    pub partition_values: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ScanSpec {
    /// Output columns in the exact order the downstream stage expects.
    pub columns: Vec<ScanColumn>,
    pub files: Vec<ScanFile>,
}

pub fn parse(plan_bytes: &[u8], split_bytes: &[u8]) -> Result<ScanSpec> {
    let read_rel = extract_read_rel(plan_bytes)?;
    let columns = parse_base_schema(&read_rel)?;
    let local_files = sb::read_rel::LocalFiles::decode(split_bytes)?;
    let files = parse_local_files(&local_files)?;
    Ok(ScanSpec { columns, files })
}

fn extract_read_rel(plan_bytes: &[u8]) -> Result<sb::ReadRel> {
    let plan = sb::Plan::decode(plan_bytes)?;
    if plan.relations.len() != 1 {
        return Err(GdfError::new(format!(
            "expected exactly one relation in the plan, got {}",
            plan.relations.len()
        )));
    }
    let rel = match plan.relations[0].rel_type.as_ref() {
        Some(sb::plan_rel::RelType::Root(root)) => root.input.as_ref(),
        Some(sb::plan_rel::RelType::Rel(rel)) => Some(rel),
        None => None,
    }
    .ok_or_else(|| GdfError::new("plan relation has no rel"))?;
    let read = match rel.rel_type.as_ref() {
        Some(RelType::Read(read)) => read.as_ref().clone(),
        other => {
            return Err(GdfError::new(format!(
                "expected a single ReadRel, got {other:?}"
            )))
        }
    };
    if read.filter.is_some() || read.best_effort_filter.is_some() {
        return Err(GdfError::new(
            "ReadRel carries a filter, which stage-1 scan offload does not support",
        ));
    }
    Ok(read)
}

fn parse_base_schema(read: &sb::ReadRel) -> Result<Vec<ScanColumn>> {
    let base_schema = read
        .base_schema
        .as_ref()
        .ok_or_else(|| GdfError::new("ReadRel has no base_schema"))?;
    let strukt = base_schema
        .r#struct
        .as_ref()
        .ok_or_else(|| GdfError::new("base_schema has no struct"))?;
    // Names are serialized in depth-first order; requiring an exact 1:1 match
    // with the top-level types rules out nested types, which stage 1 excludes.
    if base_schema.names.len() != strukt.types.len() {
        return Err(GdfError::new(format!(
            "nested types are not supported: {} names vs {} top-level types",
            base_schema.names.len(),
            strukt.types.len()
        )));
    }
    let column_types = &base_schema.column_types;
    if !column_types.is_empty() && column_types.len() != base_schema.names.len() {
        return Err(GdfError::new("column_types length mismatch"));
    }
    base_schema
        .names
        .iter()
        .zip(strukt.types.iter())
        .enumerate()
        .map(|(i, (name, tpe))| {
            let column_type = column_types.get(i).copied().unwrap_or_default();
            let is_partition = match sb::named_struct::ColumnType::try_from(column_type) {
                Ok(sb::named_struct::ColumnType::NormalCol) => false,
                Ok(sb::named_struct::ColumnType::PartitionCol) => true,
                other => {
                    return Err(GdfError::new(format!(
                        "column '{name}' has unsupported column type: {other:?}"
                    )))
                }
            };
            Ok(ScanColumn {
                field: to_arrow_field(name, tpe)?,
                is_partition,
            })
        })
        .collect()
}

fn parse_local_files(local_files: &sb::read_rel::LocalFiles) -> Result<Vec<ScanFile>> {
    local_files
        .items
        .iter()
        .map(|item| {
            match item.file_format.as_ref() {
                Some(FileFormat::Parquet(_)) => {}
                other => {
                    return Err(GdfError::new(format!(
                        "only parquet files are supported, got {other:?}"
                    )))
                }
            }
            let uri = match item.path_type.as_ref() {
                Some(PathType::UriFile(uri)) => uri,
                other => {
                    return Err(GdfError::new(format!(
                        "only uri_file paths are supported, got {other:?}"
                    )))
                }
            };
            let file_size = item
                .properties
                .as_ref()
                .map(|p| p.file_size)
                .unwrap_or_default();
            if file_size <= 0 {
                return Err(GdfError::new(format!(
                    "file '{uri}' has no valid file size in split info"
                )));
            }
            Ok(ScanFile {
                path: uri_to_local_path(uri)?,
                start: item.start,
                length: item.length,
                file_size: file_size as u64,
                partition_values: item
                    .partition_columns
                    .iter()
                    .map(|pc| (pc.key.clone(), pc.value.clone()))
                    .collect(),
            })
        })
        .collect()
}

/// Turns the (possibly URI-encoded) `uri_file` written by Spark's
/// `PartitionedFile` into an absolute local filesystem path.
fn uri_to_local_path(uri: &str) -> Result<String> {
    if let Ok(url) = url::Url::parse(uri) {
        if url.scheme() != "file" {
            return Err(GdfError::new(format!(
                "unsupported file scheme '{}' in '{uri}' (only file:// is supported)",
                url.scheme()
            )));
        }
        return url
            .to_file_path()
            .map(|p| p.to_string_lossy().into_owned())
            .map_err(|_| GdfError::new(format!("cannot convert '{uri}' to a local path")));
    }
    // A bare path without a scheme; treat it as a local path as-is.
    Ok(uri.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::substrait::r#type::{self as sbt, Kind, Nullability};

    fn string_type(nullable: bool) -> sb::Type {
        sb::Type {
            kind: Some(Kind::String(sbt::String {
                type_variation_reference: 0,
                nullability: if nullable {
                    Nullability::Nullable as i32
                } else {
                    Nullability::Required as i32
                },
            })),
        }
    }

    fn i64_type() -> sb::Type {
        sb::Type {
            kind: Some(Kind::I64(sbt::I64 {
                type_variation_reference: 0,
                nullability: Nullability::Nullable as i32,
            })),
        }
    }

    pub(crate) fn read_rel_plan(read: sb::ReadRel) -> sb::Plan {
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
    }

    fn base_schema() -> sb::NamedStruct {
        sb::NamedStruct {
            names: vec!["l_quantity".into(), "l_comment".into(), "p_date".into()],
            r#struct: Some(sbt::Struct {
                types: vec![i64_type(), string_type(true), string_type(true)],
                type_variation_reference: 0,
                nullability: Nullability::Required as i32,
                names: vec![],
            }),
            column_types: vec![
                sb::named_struct::ColumnType::NormalCol as i32,
                sb::named_struct::ColumnType::NormalCol as i32,
                sb::named_struct::ColumnType::PartitionCol as i32,
            ],
        }
    }

    fn local_files(uri: &str) -> sb::read_rel::LocalFiles {
        use sb::read_rel::local_files::file_or_files;
        sb::read_rel::LocalFiles {
            items: vec![sb::read_rel::local_files::FileOrFiles {
                path_type: Some(PathType::UriFile(uri.to_string())),
                file_format: Some(FileFormat::Parquet(file_or_files::ParquetReadOptions {})),
                start: 4,
                length: 100,
                properties: Some(file_or_files::FileProperties {
                    file_size: 104,
                    modification_time: 0,
                }),
                partition_columns: vec![file_or_files::PartitionColumn {
                    key: "p_date".into(),
                    value: "2024-01-01".into(),
                }],
                ..Default::default()
            }],
            advanced_extension: None,
        }
    }

    #[test]
    fn parses_read_rel_and_split() {
        let plan = read_rel_plan(sb::ReadRel {
            base_schema: Some(base_schema()),
            ..Default::default()
        });
        let spec = parse(
            &plan.encode_to_vec(),
            &local_files("file:///tmp/gluten%20test/part-0.parquet").encode_to_vec(),
        )
        .unwrap();
        assert_eq!(spec.columns.len(), 3);
        assert!(!spec.columns[0].is_partition);
        assert!(spec.columns[2].is_partition);
        assert_eq!(spec.files.len(), 1);
        assert_eq!(spec.files[0].path, "/tmp/gluten test/part-0.parquet");
        assert_eq!(spec.files[0].start, 4);
        assert_eq!(spec.files[0].file_size, 104);
        assert_eq!(spec.files[0].partition_values["p_date"], "2024-01-01");
    }

    #[test]
    fn rejects_filtered_read_rel() {
        let plan = read_rel_plan(sb::ReadRel {
            base_schema: Some(base_schema()),
            filter: Some(Box::default()),
            ..Default::default()
        });
        let err = parse(
            &plan.encode_to_vec(),
            &local_files("file:///tmp/a.parquet").encode_to_vec(),
        )
        .unwrap_err();
        assert!(err.0.contains("filter"), "{err}");
    }

    #[test]
    fn rejects_non_parquet() {
        let mut files = local_files("file:///tmp/a.orc");
        files.items[0].file_format = Some(FileFormat::Orc(
            sb::read_rel::local_files::file_or_files::OrcReadOptions {},
        ));
        let plan = read_rel_plan(sb::ReadRel {
            base_schema: Some(base_schema()),
            ..Default::default()
        });
        let err = parse(&plan.encode_to_vec(), &files.encode_to_vec()).unwrap_err();
        assert!(err.0.contains("parquet"), "{err}");
    }
}
