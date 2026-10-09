---
layout: page
title: Gluten For Flink with Velox Backend
nav_order: 1
---

# Supported Version

| Type  | Version                      |
|-------|------------------------------|
| Flink | 1.19.3                       |
| OS    | Ubuntu20.04/22.04, Centos7/8 |
| jdk   | openjdk11/jdk17              |
| scala | 2.12                         |

# Prerequisite

Currently, with static build Gluten+Flink+Velox backend supports all the Linux OSes, but is only tested on **Ubuntu20.04**. With dynamic build, Gluten+Velox backend support **Ubuntu20.04/Ubuntu22.04/Centos7/Centos8** and their variants.

Currently, the officially supported Flink version is 1.19.3.

We need to set up the `JAVA_HOME` env. Currently, Gluten supports **java 11** and **java 17**.

**For x86_64**

```bash
## make sure jdk11 is used
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

**For aarch64**

```bash
## make sure jdk11 is used
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
export PATH=$JAVA_HOME/bin:$PATH
```

**Get Velox4j**

Gluten for Flink depends on [Velox4j](https://github.com/velox4j/velox4j) to call velox. This is an experimental feature.
You need to get the Velox4j code, and compile it first.

As some features have not been committed to upstream, you have to use the following fork to run it first.

```bash
## fetch velox4j code
git clone -b gluten-20260829 https://github.com/bigo-sg/velox4j.git
cd velox4j
git reset --hard 26c7715278e6f6795084f6334998cb2ce382f7aa
mvn clean install -DskipTests -Dgpg.skip -Dspotless.skip=true
```
**Get gluten**

```bash
## config maven, like proxy in ~/.m2/settings.xml

## fetch gluten code
git clone https://github.com/apache/gluten.git
```

# Build Gluten Flink with Velox Backend

```
cd /path/to/gluten/gluten-flink
mvn clean package -Dmaven.test.skip=true
```

## Build the bundle jar

`gluten-flink/dev/package.sh` builds velox4j (using the pinned fork, commit and patch above) and
gluten-flink, and packages everything into a single jar with velox4j's native libraries and all
third-party Java dependencies included:

```bash
cd /path/to/gluten
./gluten-flink/dev/package.sh
# Output: gluten-flink/bundle/target/gluten-flink-bundle-1.8.0-SNAPSHOT.jar
```

Use `--velox4j_home=/path/to/velox4j` to build an existing velox4j checkout, or
`--build_velox4j=OFF` to reuse the velox4j already installed in the local maven repository.
The native libraries are built for the OS the script runs on, so build the bundle on the same
OS that the Flink cluster runs.

# Run Unit Tests
**Get Nexmark**
```shell
git clone https://github.com/nexmark/nexmark.git
cd nexmark
mvn clean install -DskipTests
```
**Run Tests**
```shell
cd /path/to/gluten/gluten-flink
mvn test
``` 

## Submit the Flink SQL job

Submit test script from `flink run`. You can use the `StreamSQLExample` as an example. 

### Flink local cluster

After deploying Flink binaries, copy the bundle jar into a directory under `$FLINK_HOME`:

```shell
export GLUTEN_FLINK_HOME=
export FLINK_HOME=

mkdir -p $FLINK_HOME/gluten_lib
cp $GLUTEN_FLINK_HOME/bundle/target/gluten-flink-bundle-1.8.0-SNAPSHOT.jar $FLINK_HOME/gluten_lib/
```

And make it loaded before flink libraries.

#### How to make sure gluten classes loaded first in Flink?

Gluten classes need to be loaded first in Flink, 
you can modify the constructFlinkClassPath function in `$FLINK_HOME/bin/config.sh` like this: 

```
GLUTEN_JAR="$FLINK_HOME/gluten_lib/gluten-flink-bundle-1.8.0-SNAPSHOT.jar:"
echo "$GLUTEN_JAR""$FLINK_CLASSPATH""$FLINK_DIST"
```

Then you can go to flink binary path and use the below scripts to
submit the example job.

```bash
cd $FLINK_HOME
bin/start-cluster.sh
bin/flink run examples/table/StreamSQLExample.jar
```

Then you can get the result in `log/flink-*-taskexecutor-*.out`.
And you can see an operator named `gluten-cal` from the web frontend of your flink job.

**Notice: current this example will cause npe until  [issue-10315](https://github.com/apache/gluten/issues/10315) get resolved.**

#### All operators executed by native
Another example supports all operators executed by native. 
You can use the data-generator.sql under dev directory.

```bash
bin/sql-client.sh -f data-generator.sql
```

### Flink Yarn per job mode

TODO

### RocksDB State

**Get & compile RocksDB**
```bash
git clone -b FRocksDB-6.20.3 https://github.com/ververica/frocksdb.git
cd frocksdb
make rocksdbjava -i
```

**Config RocksDB backend**
- copy compiled jar package to `${FLINK_HOME}/gluten_lib` directory.
    ```bash
    cp ${ROCKSDB_COMPILE_DIR}/java/target/rocksdbjni-6.20.3-linux64.jar ${FLINK_HOME}/gluten_lib
    ```
- modify `${FLINK_HOME}/bin/config.sh` as follows
    ```
    GLUTEN_JAR="$FLINK_HOME/gluten_lib/gluten-flink-bundle-1.8.0-SNAPSHOT.jar:$FLINK_HOME/gluten_lib/rocksdbjni-6.20.3-linux64.jar"
    echo "$GLUTEN_JAR""$FLINK_CLASSPATH""$FLINK_DIST"
    ```
- set rocksdb config in `${FLINK_HOME}/conf/config.yaml`
    ```
    state.backend.type: rocksdb
    ```

## Performance
We are working on supporting the [Nexmark](https://github.com/nexmark/nexmark) benchmark for Flink.
Now the q0 has been supported.

To run it yourself, `gluten-flink/benchmark/nexmark/run.sh` sets up a local Flink cluster and runs
the Nexmark queries with and without Gluten, then reports the speedup per query. See
[the benchmark README](../benchmark/nexmark/README.md).

Results show that running with gluten can be 2.x times faster than Flink.

Result using gluten (will support TPS metric soon):
```
-------------------------------- Nexmark Results --------------------------------

+------+-----------------+--------+----------+-----------------+--------------+-----------------+
| Query| Events Num      | Cores  | Time(s)  | Cores * Time(s) | Throughput   | Throughput/Cores|
+------+-----------------+--------+----------+-----------------+--------------+-----------------+
|q0    |100,000,000      |NaN     |161.428   |NaN              |619.47 K/s    |0/s              |
|Total |100,000,000      |NaN     |161.428   |NaN              |619.47 K/s    |0/s              |
+------+-----------------+--------+----------+-----------------+--------------+-----------------+
```

Result using Flink:
```
-------------------------------- Nexmark Results --------------------------------

+------+-----------------+--------+----------+-----------------+--------------+-----------------+
| Query| Events Num      | Cores  | Time(s)  | Cores * Time(s) | Throughput   | Throughput/Cores|
+------+-----------------+--------+----------+-----------------+--------------+-----------------+
|q0    |100,000,000      |1.21    |462.069   |558.210          |216.42 K/s    |179.14/s         |
|Total |100,000,000      |1.208   |462.069   |558.210          |216.42 K/s    |179.14/s         |
+------+-----------------+--------+----------+-----------------+--------------+-----------------+
```
We are still optimizing it.

## Notes:
The bundle jar includes velox4j and its dependencies (guava, jackson, arrow, flatbuffers, commons-io)
without relocation, because velox4j and arrow-c-data look up Java classes by name from native code.
If a Flink job jar ships conflicting versions of these libraries, the versions in the bundle win.
