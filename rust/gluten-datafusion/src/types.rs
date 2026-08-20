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

use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};

use crate::error::{GdfError, Result};
use crate::proto::substrait as sb;
use crate::proto::substrait::r#type::{Kind, Nullability};

/// Maps a Substrait type emitted by Gluten's `ConverterUtils.getTypeNode` to
/// an Arrow field. Only the flat types the stage-1 JVM validator lets through
/// are supported; anything else is an error rather than a silent mismatch.
pub fn to_arrow_field(name: &str, t: &sb::Type) -> Result<Field> {
    let kind = t
        .kind
        .as_ref()
        .ok_or_else(|| GdfError::new(format!("column '{name}' has no substrait type kind")))?;
    let (data_type, nullability) = match kind {
        Kind::Bool(n) => (DataType::Boolean, n.nullability),
        Kind::I8(n) => (DataType::Int8, n.nullability),
        Kind::I16(n) => (DataType::Int16, n.nullability),
        Kind::I32(n) => (DataType::Int32, n.nullability),
        Kind::I64(n) => (DataType::Int64, n.nullability),
        Kind::Fp32(n) => (DataType::Float32, n.nullability),
        Kind::Fp64(n) => (DataType::Float64, n.nullability),
        Kind::String(n) => (DataType::Utf8, n.nullability),
        Kind::Binary(n) => (DataType::Binary, n.nullability),
        Kind::Date(n) => (DataType::Date32, n.nullability),
        Kind::FixedChar(c) => (DataType::Utf8, c.nullability),
        Kind::Varchar(c) => (DataType::Utf8, c.nullability),
        Kind::Decimal(d) => (
            DataType::Decimal128(d.precision as u8, d.scale as i8),
            d.nullability,
        ),
        // Spark TimestampType: microseconds since epoch, UTC-normalized.
        Kind::PrecisionTimestampTz(ts) if ts.precision == 6 => (
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ts.nullability,
        ),
        // Spark TimestampNTZType.
        Kind::PrecisionTimestamp(ts) if ts.precision == 6 => (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            ts.nullability,
        ),
        other => {
            return Err(GdfError::new(format!(
                "column '{name}' has unsupported substrait type: {other:?}"
            )))
        }
    };
    let nullable = !matches!(
        Nullability::try_from(nullability),
        Ok(Nullability::Required)
    );
    Ok(Field::new(name, data_type, nullable))
}
