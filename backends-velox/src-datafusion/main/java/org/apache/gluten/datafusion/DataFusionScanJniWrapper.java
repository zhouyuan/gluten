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
package org.apache.gluten.datafusion;

/**
 * JNI bridge into libgluten_datafusion (Rust). The native side executes a Substrait plan holding a
 * single ReadRel with Apache DataFusion and streams Arrow batches back through the Arrow C Data
 * Interface.
 */
public final class DataFusionScanJniWrapper {
  private DataFusionScanJniWrapper() {}

  /**
   * Opens a scan over one task's split.
   *
   * @param plan serialized {@code substrait.Plan} containing exactly one ReadRel
   * @param split serialized {@code substrait.ReadRel.LocalFiles} for this task
   * @param confJson UTF-8 JSON options, e.g. {@code {"batch_size":4096,"threads":0}}
   * @return native scan handle
   */
  public static native long open(byte[] plan, byte[] split, byte[] confJson);

  /**
   * Fetches the next batch. On success the exported {@code ArrowSchema}/{@code ArrowArray} structs
   * are moved into the given JVM-allocated shells (which the caller owns and must import/release).
   *
   * @return true if a batch was produced, false on end of stream
   */
  public static native boolean next(long handle, long cSchemaAddress, long cArrayAddress);

  /** Closes the scan and releases native resources. Safe to call with 0. */
  public static native void close(long handle);
}
