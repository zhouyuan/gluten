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

use std::sync::OnceLock;

use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Process-global tokio runtime shared by all concurrent scans. A single
/// bounded pool keeps the native thread count independent of the number of
/// Spark task threads. The `threads` hint is honored by whichever scan
/// initializes the runtime first (0 = one thread per core).
pub fn global(threads: usize) -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if threads > 0 {
            builder.worker_threads(threads);
        }
        builder
            .thread_name("gluten-datafusion")
            .enable_all()
            .build()
            .expect("failed to build tokio runtime")
    })
}
