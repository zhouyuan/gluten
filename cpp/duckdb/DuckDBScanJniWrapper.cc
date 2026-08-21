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

// JNI entry points for org.apache.gluten.duckdb.DuckDBScanJniWrapper.

#include <jni.h>

#include <exception>
#include <string>

#include "DuckDBScan.h"

namespace {

void throwRuntimeException(JNIEnv* env, const std::string& message) {
  // If an exception is already pending, keep it rather than replacing it.
  if (env->ExceptionCheck()) {
    return;
  }
  jclass cls = env->FindClass("java/lang/RuntimeException");
  if (cls != nullptr) {
    env->ThrowNew(cls, message.c_str());
  }
}

std::string toString(JNIEnv* env, jstring string) {
  if (string == nullptr) {
    return {};
  }
  const char* chars = env->GetStringUTFChars(string, nullptr);
  std::string result(chars);
  env->ReleaseStringUTFChars(string, chars);
  return result;
}

std::string toBytes(JNIEnv* env, jbyteArray bytes) {
  jsize length = env->GetArrayLength(bytes);
  std::string result(static_cast<size_t>(length), '\0');
  env->GetByteArrayRegion(bytes, 0, length, reinterpret_cast<jbyte*>(result.data()));
  return result;
}

} // namespace

extern "C" {

JNIEXPORT jobjectArray JNICALL
Java_org_apache_gluten_duckdb_DuckDBScanJniWrapper_describeParquet(JNIEnv* env, jclass, jstring path) {
  try {
    auto names = gluten::duckdbDescribeParquet(toString(env, path));
    jobjectArray result =
        env->NewObjectArray(static_cast<jsize>(names.size()), env->FindClass("java/lang/String"), nullptr);
    for (jsize i = 0; i < static_cast<jsize>(names.size()); i++) {
      env->SetObjectArrayElement(result, i, env->NewStringUTF(names[i].c_str()));
    }
    return result;
  } catch (const std::exception& e) {
    throwRuntimeException(env, e.what());
    return nullptr;
  }
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_duckdb_DuckDBScanJniWrapper_open(
    JNIEnv* env,
    jclass,
    jbyteArray plan,
    jlong threads,
    jstring memoryLimit,
    jstring substraitExtensionPath) {
  try {
    gluten::DuckDBScanOptions options;
    options.threads = threads;
    options.memoryLimit = toString(env, memoryLimit);
    options.substraitExtensionPath = toString(env, substraitExtensionPath);
    auto scan = std::make_unique<gluten::DuckDBScan>(options);
    scan->execute(toBytes(env, plan));
    return reinterpret_cast<jlong>(scan.release());
  } catch (const std::exception& e) {
    throwRuntimeException(env, e.what());
    return 0;
  }
}

JNIEXPORT jboolean JNICALL Java_org_apache_gluten_duckdb_DuckDBScanJniWrapper_next(
    JNIEnv* env,
    jclass,
    jlong handle,
    jlong cSchemaAddress,
    jlong cArrayAddress) {
  try {
    auto* scan = reinterpret_cast<gluten::DuckDBScan*>(handle);
    if (scan == nullptr) {
      throw std::runtime_error("gluten-duckdb: scan handle is null");
    }
    return scan->next(
               reinterpret_cast<struct ArrowSchema*>(cSchemaAddress),
               reinterpret_cast<struct ArrowArray*>(cArrayAddress))
        ? JNI_TRUE
        : JNI_FALSE;
  } catch (const std::exception& e) {
    throwRuntimeException(env, e.what());
    return JNI_FALSE;
  }
}

JNIEXPORT void JNICALL
Java_org_apache_gluten_duckdb_DuckDBScanJniWrapper_close(JNIEnv* env, jclass, jlong handle) {
  try {
    delete reinterpret_cast<gluten::DuckDBScan*>(handle);
  } catch (const std::exception& e) {
    throwRuntimeException(env, e.what());
  }
}

} // extern "C"
