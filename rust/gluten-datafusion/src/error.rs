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

use std::fmt;

/// Crate-wide error. Rendered into a `RuntimeException` message at the JNI
/// boundary, so it only carries a human-readable string.
#[derive(Debug)]
pub struct GdfError(pub String);

impl GdfError {
    pub fn new(msg: impl Into<String>) -> Self {
        GdfError(msg.into())
    }
}

impl fmt::Display for GdfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "gluten-datafusion: {}", self.0)
    }
}

impl std::error::Error for GdfError {}

impl From<prost::DecodeError> for GdfError {
    fn from(e: prost::DecodeError) -> Self {
        GdfError(format!("failed to decode substrait protobuf: {e}"))
    }
}

impl From<datafusion::error::DataFusionError> for GdfError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        GdfError(format!("datafusion error: {e}"))
    }
}

impl From<datafusion::arrow::error::ArrowError> for GdfError {
    fn from(e: datafusion::arrow::error::ArrowError) -> Self {
        GdfError(format!("arrow error: {e}"))
    }
}

pub type Result<T> = std::result::Result<T, GdfError>;
