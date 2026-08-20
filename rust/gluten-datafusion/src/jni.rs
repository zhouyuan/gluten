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

//! JNI entry points for `org.apache.gluten.datafusion.DataFusionScanJniWrapper`.

use std::panic::{catch_unwind, AssertUnwindSafe};

use arrow::array::{Array, StructArray};
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use jni::objects::{JByteArray, JClass};
use jni::sys::{jboolean, jlong, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;
use serde::Deserialize;

use crate::error::{GdfError, Result};
use crate::{plan, runtime, scan};

pub struct ScanHandle {
    stream: SendableRecordBatchStream,
}

/// Session configuration serialized as JSON by the JVM side.
#[derive(Debug, Default, Deserialize)]
struct NativeConf {
    #[serde(default)]
    batch_size: Option<usize>,
    #[serde(default)]
    threads: Option<usize>,
}

fn throw(env: &mut JNIEnv, message: &str) {
    // If an exception is already pending, keep it rather than replacing it.
    if !env.exception_check().unwrap_or(false) {
        let _ = env.throw_new("java/lang/RuntimeException", message);
    }
}

fn to_vec(env: &mut JNIEnv, bytes: &JByteArray) -> Result<Vec<u8>> {
    env.convert_byte_array(bytes)
        .map_err(|e| GdfError::new(format!("failed to read byte array from JVM: {e}")))
}

/// # Safety
/// Called by the JVM with valid arguments.
#[no_mangle]
pub extern "system" fn Java_org_apache_gluten_datafusion_DataFusionScanJniWrapper_open(
    mut env: JNIEnv,
    _class: JClass,
    plan_bytes: JByteArray,
    split_bytes: JByteArray,
    conf_bytes: JByteArray,
) -> jlong {
    let result = catch_unwind(AssertUnwindSafe(|| {
        open_impl(&mut env, &plan_bytes, &split_bytes, &conf_bytes)
    }));
    match result {
        Ok(Ok(handle)) => Box::into_raw(Box::new(handle)) as jlong,
        Ok(Err(e)) => {
            throw(&mut env, &e.to_string());
            0
        }
        Err(panic) => {
            throw(&mut env, &panic_message(panic));
            0
        }
    }
}

fn open_impl(
    env: &mut JNIEnv,
    plan_bytes: &JByteArray,
    split_bytes: &JByteArray,
    conf_bytes: &JByteArray,
) -> Result<ScanHandle> {
    let plan_buf = to_vec(env, plan_bytes)?;
    let split_buf = to_vec(env, split_bytes)?;
    let conf_buf = to_vec(env, conf_bytes)?;
    let conf: NativeConf = if conf_buf.is_empty() {
        NativeConf::default()
    } else {
        serde_json::from_slice(&conf_buf)
            .map_err(|e| GdfError::new(format!("invalid native conf json: {e}")))?
    };

    let spec = plan::parse(&plan_buf, &split_buf)?;
    let options = scan::ScanOptions {
        batch_size: conf.batch_size.unwrap_or(4096),
    };
    let rt = runtime::global(conf.threads.unwrap_or(0));
    // Enter the runtime so anything spawned while building the stream lands
    // on the shared pool.
    let _guard = rt.enter();
    let stream = scan::create_stream(&spec, &options)?;
    Ok(ScanHandle { stream })
}

/// # Safety
/// `handle` must be a live pointer returned by `open`; `c_schema_addr` and
/// `c_array_addr` must point to JVM-allocated `ArrowSchema`/`ArrowArray`
/// struct shells that this call takes over (Arrow C Data Interface move
/// semantics).
#[no_mangle]
pub extern "system" fn Java_org_apache_gluten_datafusion_DataFusionScanJniWrapper_next(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    c_schema_addr: jlong,
    c_array_addr: jlong,
) -> jboolean {
    let result = catch_unwind(AssertUnwindSafe(|| {
        next_impl(handle, c_schema_addr, c_array_addr)
    }));
    match result {
        Ok(Ok(true)) => JNI_TRUE,
        Ok(Ok(false)) => JNI_FALSE,
        Ok(Err(e)) => {
            throw(&mut env, &e.to_string());
            JNI_FALSE
        }
        Err(panic) => {
            throw(&mut env, &panic_message(panic));
            JNI_FALSE
        }
    }
}

fn next_impl(handle: jlong, c_schema_addr: jlong, c_array_addr: jlong) -> Result<bool> {
    let scan_handle = unsafe {
        (handle as *mut ScanHandle)
            .as_mut()
            .ok_or_else(|| GdfError::new("scan handle is null"))?
    };
    match runtime::global(0).block_on(scan_handle.stream.next()) {
        None => Ok(false),
        Some(batch) => {
            let batch = batch?;
            let struct_array = StructArray::from(batch);
            let (ffi_array, ffi_schema) = to_ffi(&struct_array.into_data())?;
            unsafe {
                std::ptr::write(c_schema_addr as *mut FFI_ArrowSchema, ffi_schema);
                std::ptr::write(c_array_addr as *mut FFI_ArrowArray, ffi_array);
            }
            Ok(true)
        }
    }
}

/// # Safety
/// `handle` must be a pointer returned by `open`, not yet closed, or 0.
#[no_mangle]
pub extern "system" fn Java_org_apache_gluten_datafusion_DataFusionScanJniWrapper_close(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    if handle != 0 {
        drop(unsafe { Box::from_raw(handle as *mut ScanHandle) });
    }
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    let detail = panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string());
    format!("gluten-datafusion panicked: {detail}")
}
