---
layout: page
title: Dynamic OpenSSL with FIPS Support
nav_order: 8
parent: Getting-Started
---

# Dynamic OpenSSL with FIPS Support

## Overview

By default, Gluten's vcpkg build statically links OpenSSL into the native libraries (`libvelox.so` and `libgluten.so`). However, in environments requiring FIPS (Federal Information Processing Standards) compliance or dynamic OpenSSL linking, Gluten now supports building with dynamically linked OpenSSL.

This feature is particularly useful for:
- **FIPS compliance**: Organizations requiring FIPS 140-2/140-3 validated cryptographic modules
- **Security updates**: Easier OpenSSL security patching without rebuilding Gluten
- **System integration**: Using system-provided OpenSSL libraries

## Prerequisites

- vcpkg-based build system (requires `enable_vcpkg=ON`)
- OpenSSL development libraries installed on the system
- Supported platforms: x86_64 and aarch64 Linux

## Building with Dynamic OpenSSL

### Basic Build Command

To enable dynamic OpenSSL linking with FIPS support, set the `VCPKG_DYNAMIC_OPENSSL` environment variable:

```bash
export VCPKG_DYNAMIC_OPENSSL=ON
./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON
```

### Build with Cloud Storage Support

You can combine dynamic OpenSSL with cloud storage features:

```bash
export VCPKG_DYNAMIC_OPENSSL=ON
./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON \
  --enable_s3=ON \
  --enable_gcs=ON \
  --enable_abfs=ON
```

### Architecture-Specific Builds

**For x86_64:**
```bash
export VCPKG_DYNAMIC_OPENSSL=ON
./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON
```

**For aarch64:**
```bash
export CPU_TARGET="aarch64"
export VCPKG_DYNAMIC_OPENSSL=ON
./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON
```

## How It Works

When `VCPKG_DYNAMIC_OPENSSL=ON` is set:

1. **vcpkg Configuration**: The `dynamic-openssl` feature is enabled in `vcpkg.json`, which includes OpenSSL with FIPS support
2. **Triplet Override**: Custom vcpkg triplets (`x64-linux-avx.cmake` and `arm64-linux-neon.cmake`) detect the environment variable and switch OpenSSL from static to dynamic linkage
3. **Build Process**: The `init.sh` script adds the `--x-feature=dynamic-openssl` flag to vcpkg install
4. **Library Packaging**: OpenSSL shared libraries are excluded from static linking but must be available at runtime

## Runtime Requirements

### OpenSSL Library Deployment

With dynamic OpenSSL, you must ensure OpenSSL shared libraries are available on all Spark executor nodes:

**Option 1: System Installation**
```bash
# Ubuntu/Debian
sudo apt-get install libssl3

# CentOS/RHEL 8+
sudo yum install openssl-libs

# CentOS/RHEL 7
sudo yum install openssl11-libs
```

**Option 2: Custom Library Path**

If using a custom OpenSSL installation:

```bash
# Set library path in Spark configuration
--conf spark.executorEnv.LD_LIBRARY_PATH=/path/to/openssl/lib
--conf spark.driverEnv.LD_LIBRARY_PATH=/path/to/openssl/lib
```

### FIPS Mode Configuration

To enable FIPS mode for OpenSSL at runtime:

1. **Configure OpenSSL FIPS Module**:
```bash
# Verify FIPS module is available
openssl list -providers

# Enable FIPS mode in OpenSSL configuration
# Edit /etc/ssl/openssl.cnf or create a custom config
```

2. **Set Environment Variables**:
```bash
--conf spark.executorEnv.OPENSSL_CONF=/path/to/openssl-fips.cnf
--conf spark.driverEnv.OPENSSL_CONF=/path/to/openssl-fips.cnf
```

## Verification

### Check OpenSSL Linkage

After building, verify that OpenSSL is dynamically linked:

```bash
# Check libgluten.so
ldd cpp/build/releases/libgluten.so | grep ssl

# Expected output (dynamic):
# libssl.so.3 => /lib/x86_64-linux-gnu/libssl.so.3
# libcrypto.so.3 => /lib/x86_64-linux-gnu/libcrypto.so.3

# Check libvelox.so
ldd cpp/build/releases/libvelox.so | grep ssl
```

### Verify FIPS Mode at Runtime

Add the following to your Spark application to verify FIPS mode:

```scala
// Check if FIPS mode is enabled
import java.security.Security
val fipsEnabled = Security.getProviders.exists(_.getName.contains("FIPS"))
println(s"FIPS Mode Enabled: $fipsEnabled")
```

## Comparison: Static vs Dynamic OpenSSL

| Aspect | Static OpenSSL (Default) | Dynamic OpenSSL |
|--------|-------------------------|-----------------|
| **Build Flag** | `VCPKG_DYNAMIC_OPENSSL=OFF` (default) | `VCPKG_DYNAMIC_OPENSSL=ON` |
| **Deployment** | Self-contained JAR | Requires OpenSSL on nodes |
| **FIPS Support** | Limited | Full FIPS 140-2/140-3 support |
| **Security Updates** | Requires rebuild | System package updates |
| **Binary Size** | Larger (~2-3 MB more) | Smaller |
| **Portability** | High (no external deps) | Lower (requires compatible OpenSSL) |

## Troubleshooting

### OpenSSL Not Found at Runtime

**Error**: `libssl.so.3: cannot open shared object file`

**Solution**:
```bash
# Install OpenSSL on all executor nodes
sudo apt-get install libssl3  # Ubuntu/Debian
sudo yum install openssl-libs  # CentOS/RHEL

# Or set LD_LIBRARY_PATH
--conf spark.executorEnv.LD_LIBRARY_PATH=/usr/local/lib
```

### FIPS Mode Not Enabled

**Error**: FIPS validation fails or FIPS provider not loaded

**Solution**:
1. Verify OpenSSL FIPS module installation:
   ```bash
   openssl list -providers
   ```
2. Check OpenSSL configuration file points to FIPS module
3. Ensure `OPENSSL_CONF` environment variable is set correctly

### Version Mismatch

**Error**: OpenSSL version incompatibility

**Solution**:
- Ensure all nodes have the same OpenSSL version
- Check vcpkg's OpenSSL version matches system version
- Consider using static build if version consistency is difficult

## Build System Details

### Modified Files

The dynamic OpenSSL feature modifies the following build system files:

- `dev/vcpkg/vcpkg.json`: Adds `dynamic-openssl` feature with FIPS-enabled OpenSSL
- `dev/vcpkg/init.sh`: Detects `VCPKG_DYNAMIC_OPENSSL` and adds feature flag
- `dev/vcpkg/env.sh`: Exports environment variable for build scripts
- `dev/vcpkg/triplets/x64-linux-avx.cmake`: Conditionally switches OpenSSL linkage
- `dev/vcpkg/triplets/arm64-linux-neon.cmake`: Conditionally switches OpenSSL linkage

### vcpkg Feature Definition

The `dynamic-openssl` feature in `vcpkg.json`:

```json
"dynamic-openssl": {
  "description": "Enable dynamic OpenSSL with fips feature",
  "dependencies": [
    {
      "name": "openssl",
      "features": ["fips"]
    }
  ]
}
```

## Best Practices

1. **Use Static Build by Default**: Unless FIPS compliance or dynamic linking is required, use the default static build for easier deployment

2. **Test FIPS Mode**: Thoroughly test your workload with FIPS mode enabled, as it may impact performance and compatibility

3. **Version Consistency**: Ensure all cluster nodes have the same OpenSSL version to avoid runtime issues

4. **Security Updates**: With dynamic linking, establish a process for applying OpenSSL security updates across the cluster

5. **Documentation**: Document your OpenSSL configuration and FIPS requirements for operations teams

## Related Documentation

- [Velox Build Guide](Velox.md)
- [Velox S3 Support](VeloxS3.md) - S3 uses OpenSSL for TLS
- [Velox ABFS Support](VeloxABFS.md) - ABFS uses OpenSSL for TLS
- [Velox GCS Support](VeloxGCS.md) - GCS uses OpenSSL for TLS

## References

- [OpenSSL FIPS Module](https://www.openssl.org/docs/fips.html)
- [FIPS 140-2 Standard](https://csrc.nist.gov/publications/detail/fips/140/2/final)
- [vcpkg Documentation](https://vcpkg.io/)