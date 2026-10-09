#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Runs the Nexmark benchmark on a local standalone Flink cluster, once with vanilla Flink and once
# with the Gluten bundle on the classpath, then writes a comparison report. See README.md.

set -euo pipefail

BENCH_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd)
GLUTEN_FLINK_DIR=$(cd "$BENCH_DIR/../.."; pwd)
GLUTEN_DIR=$(cd "$GLUTEN_FLINK_DIR/.."; pwd)
MVN="$GLUTEN_DIR/build/mvn"

FLINK_VERSION=$(sed -n 's:.*<flink.version>\(.*\)</flink.version>.*:\1:p' "$GLUTEN_FLINK_DIR/pom.xml")
FLINK_TGZ_NAME="flink-$FLINK_VERSION-bin-scala_2.12.tgz"
# The CDN is much faster but only hosts current releases, so fall back to the archive. Set
# FLINK_URL to use a specific mirror. The checksum always comes from the archive, which has all.
FLINK_URLS=${FLINK_URL:-"https://dlcdn.apache.org/flink/flink-$FLINK_VERSION/$FLINK_TGZ_NAME https://archive.apache.org/dist/flink/flink-$FLINK_VERSION/$FLINK_TGZ_NAME"}
FLINK_SHA512_URL="https://archive.apache.org/dist/flink/flink-$FLINK_VERSION/$FLINK_TGZ_NAME.sha512"
NEXMARK_REPO=${NEXMARK_REPO:-https://github.com/nexmark/nexmark.git}
NEXMARK_COMMIT=${NEXMARK_COMMIT:-6b3646c3baec701f1fa74baf938d235f742e5d3c}

# The queries gluten-flink's NexmarkTest covers (q6, q13 and q23 are not supported yet).
DEFAULT_QUERIES=q0,q1,q2,q3,q4,q5,q7,q8,q9,q10,q11,q12,q14,q15,q16,q17,q18,q19,q20,q21,q22

MODES=vanilla,gluten
QUERIES=$DEFAULT_QUERIES
EVENTS_NUM=10000000
TPS=10000000
WARMUP_EVENTS_NUM=0
WARMUP_DURATION=0s
NUM_TM=1
SLOTS=1
PARALLELISM=1
TM_MEMORY=4g
JM_MEMORY=2g
STATE_BACKEND=hashmap
REST_PORT=8081
METRIC_PORT=9098
MONITOR_DELAY=3s
MONITOR_INTERVAL=1s
QUERY_TIMEOUT=1800
BUNDLE_JAR=""
FLINK_TGZ=""
WORK_DIR="$BENCH_DIR/work"
RESULT_DIR=""
REBUILD_NEXMARK=OFF

usage() {
  cat <<EOF
Usage: $0 [options]
  --modes=vanilla,gluten      Which sides to run (default: $MODES).
  --queries=q0,q1,...         Queries to run (default: all queries gluten-flink supports).
  --events_num=N              Events generated per query (default: $EVENTS_NUM).
  --tps=N                     Source rate limit in events/s (default: $TPS).
  --warmup_events_num=N       Warmup events before each query, 0 disables (default: $WARMUP_EVENTS_NUM).
  --warmup_duration=D         Max warmup time, e.g. 60s (default: $WARMUP_DURATION).
  --num_tm=N                  TaskManagers in the mini cluster (default: $NUM_TM).
  --slots=N                   Slots per TaskManager (default: $SLOTS).
  --parallelism=N             Default job parallelism (default: $PARALLELISM).
  --tm_memory=SIZE            TaskManager process size (default: $TM_MEMORY).
  --jm_memory=SIZE            JobManager process size (default: $JM_MEMORY).
  --state_backend=NAME        hashmap or rocksdb (default: $STATE_BACKEND).
  --rest_port=PORT            Flink REST port (default: $REST_PORT).
  --query_timeout=SECONDS     Per-query timeout (default: $QUERY_TIMEOUT).
  --bundle=PATH               Gluten bundle jar (default: gluten-flink/bundle/target/gluten-flink-bundle-*.jar).
  --flink_tgz=PATH            Use a local Flink $FLINK_VERSION binary tarball instead of downloading it.
  --work_dir=DIR              Downloads, builds and cluster installs (default: $WORK_DIR).
  --result_dir=DIR            Where to write results (default: $BENCH_DIR/results/<timestamp>).
  --rebuild_nexmark=ON        Rebuild Nexmark even if it was built before.
EOF
}

for arg in "$@"; do
  case $arg in
    --modes=*) MODES="${arg#*=}" ;;
    --queries=*) QUERIES="${arg#*=}" ;;
    --events_num=*) EVENTS_NUM="${arg#*=}" ;;
    --tps=*) TPS="${arg#*=}" ;;
    --warmup_events_num=*) WARMUP_EVENTS_NUM="${arg#*=}" ;;
    --warmup_duration=*) WARMUP_DURATION="${arg#*=}" ;;
    --num_tm=*) NUM_TM="${arg#*=}" ;;
    --slots=*) SLOTS="${arg#*=}" ;;
    --parallelism=*) PARALLELISM="${arg#*=}" ;;
    --tm_memory=*) TM_MEMORY="${arg#*=}" ;;
    --jm_memory=*) JM_MEMORY="${arg#*=}" ;;
    --state_backend=*) STATE_BACKEND="${arg#*=}" ;;
    --rest_port=*) REST_PORT="${arg#*=}" ;;
    --query_timeout=*) QUERY_TIMEOUT="${arg#*=}" ;;
    --bundle=*) BUNDLE_JAR="${arg#*=}" ;;
    --flink_tgz=*) FLINK_TGZ="${arg#*=}" ;;
    --work_dir=*) WORK_DIR="${arg#*=}" ;;
    --result_dir=*) RESULT_DIR="${arg#*=}" ;;
    --rebuild_nexmark=*) REBUILD_NEXMARK="${arg#*=}" ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $arg"; usage; exit 1 ;;
  esac
done

RESULT_DIR=${RESULT_DIR:-$BENCH_DIR/results/$(date +%Y%m%d-%H%M%S)}
mkdir -p "$WORK_DIR" "$RESULT_DIR"
WORK_DIR=$(cd "$WORK_DIR"; pwd)
RESULT_DIR=$(cd "$RESULT_DIR"; pwd)

log() {
  echo "[$(date '+%F %T')] $*" >&2
}

# --- REST helpers ------------------------------------------------------------------------------

rest() {
  curl -sf "http://localhost:$REST_PORT$1"
}

json() {
  python3 -I -c "import json, sys; d = json.load(sys.stdin); $1"
}

job_ids() {
  rest /jobs | json 'print("\n".join(j["id"] for j in d["jobs"]))'
}

cancel_running_jobs() {
  local id
  for id in $(rest /jobs | json 'print("\n".join(j["id"] for j in d["jobs"] if j["status"] in ("RUNNING", "CREATED", "INITIALIZING", "RESTARTING")))'); do
    log "Cancelling job $id"
    curl -sf -X PATCH "http://localhost:$REST_PORT/jobs/$id?mode=cancel" > /dev/null || true
  done
}

# --- Preparation -------------------------------------------------------------------------------

render() {
  # render <template> <output> KEY=VALUE...
  python3 -I - "$@" <<'EOF'
import re
import sys

src, dst, *pairs = sys.argv[1:]
with open(src) as f:
    text = f.read()
for pair in pairs:
    key, value = pair.split("=", 1)
    text = text.replace("@" + key + "@", value)
left = sorted(set(re.findall(r"@[A-Z_]+@", text)))
if left:
    sys.exit("Unresolved placeholders in %s: %s" % (src, ", ".join(left)))
with open(dst, "w") as f:
    f.write(text)
EOF
}

sha512_check() {
  if command -v sha512sum > /dev/null; then
    (cd "$(dirname "$1")" && sha512sum -c "$(basename "$1").sha512")
  else
    (cd "$(dirname "$1")" && shasum -a 512 -c "$(basename "$1").sha512")
  fi
}

prepare_flink() {
  if [ -n "$FLINK_TGZ" ]; then
    [ -f "$FLINK_TGZ" ] || { echo "Flink tarball not found: $FLINK_TGZ"; exit 1; }
    return
  fi
  FLINK_TGZ="$WORK_DIR/downloads/$FLINK_TGZ_NAME"
  if [ -f "$FLINK_TGZ" ]; then
    return
  fi
  mkdir -p "$WORK_DIR/downloads"
  local url downloaded=""
  for url in $FLINK_URLS; do
    # A HEAD request first, so a mirror without this version is skipped quickly.
    curl -fsIL "$url" > /dev/null 2>&1 || { log "Flink $FLINK_VERSION is not on $url"; continue; }
    log "Downloading Flink $FLINK_VERSION from $url"
    if curl -fL --retry 3 -o "$FLINK_TGZ.part" "$url"; then
      downloaded=1
      break
    fi
  done
  [ -n "$downloaded" ] || { echo "Could not download Flink $FLINK_VERSION from: $FLINK_URLS"; exit 1; }
  curl -fsL --retry 3 -o "$FLINK_TGZ.sha512" "$FLINK_SHA512_URL"
  mv "$FLINK_TGZ.part" "$FLINK_TGZ"
  sha512_check "$FLINK_TGZ" || { rm -f "$FLINK_TGZ"; exit 1; }
}

prepare_nexmark() {
  NEXMARK_DIST="$WORK_DIR/nexmark-dist"
  if [ -d "$NEXMARK_DIST" ] && [ "$REBUILD_NEXMARK" != "ON" ]; then
    return
  fi
  local src="$WORK_DIR/src/nexmark"
  if [ ! -d "$src/.git" ]; then
    mkdir -p "$WORK_DIR/src"
    git clone "$NEXMARK_REPO" "$src"
  fi
  git -C "$src" fetch origin
  git -C "$src" checkout -q "$NEXMARK_COMMIT"
  # Upstream targets Flink 2.0; build against the version gluten-flink runs on. Its tests do not
  # compile against 1.19, hence maven.test.skip.
  log "Building Nexmark $NEXMARK_COMMIT against Flink $FLINK_VERSION"
  (cd "$src" && "$MVN" -q clean package -pl nexmark-flink -am -Dmaven.test.skip=true -Dflink.version="$FLINK_VERSION")
  rm -rf "$NEXMARK_DIST"
  cp -R "$src/nexmark-flink/target/nexmark-flink-bin/nexmark-flink" "$NEXMARK_DIST"
}

find_bundle() {
  if [ -z "$BUNDLE_JAR" ]; then
    BUNDLE_JAR=$(ls "$GLUTEN_FLINK_DIR"/bundle/target/gluten-flink-bundle-*.jar 2> /dev/null | grep -v original- | head -1 || true)
  fi
  if [ ! -f "$BUNDLE_JAR" ]; then
    echo "Gluten bundle jar not found. Build it with gluten-flink/dev/package.sh or pass --bundle=PATH."
    exit 1
  fi
}

# Installs a fresh Flink and Nexmark for one mode, so runs never share state or classpath.
setup_mode() {
  local mode=$1
  FLINK_HOME="$WORK_DIR/$mode/flink"
  NEXMARK_HOME="$WORK_DIR/$mode/nexmark"
  rm -rf "$WORK_DIR/$mode"
  mkdir -p "$WORK_DIR/$mode/tmp"

  tar xzf "$FLINK_TGZ" -C "$WORK_DIR/$mode"
  mv "$WORK_DIR/$mode/flink-$FLINK_VERSION" "$FLINK_HOME"
  cp -R "$NEXMARK_DIST" "$NEXMARK_HOME"
  cp "$NEXMARK_HOME"/lib/nexmark-flink-*.jar "$FLINK_HOME/lib/"

  local dist_java_opts
  dist_java_opts=$(sed -n 's/^ *all: *//p' "$FLINK_HOME/conf/config.yaml" | head -1)
  local java_opts="$dist_java_opts --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens=java.base/sun.reflect.generics.repository=ALL-UNNAMED"
  render "$BENCH_DIR/conf/config.yaml.template" "$FLINK_HOME/conf/config.yaml" \
    JAVA_OPTS="$java_opts" JM_MEMORY="$JM_MEMORY" TM_MEMORY="$TM_MEMORY" SLOTS="$SLOTS" \
    PARALLELISM="$PARALLELISM" REST_PORT="$REST_PORT" STATE_BACKEND="$STATE_BACKEND" \
    TMP_DIR="$WORK_DIR/$mode/tmp"
  echo localhost > "$FLINK_HOME/conf/workers"
  echo "localhost:$REST_PORT" > "$FLINK_HOME/conf/masters"

  render "$BENCH_DIR/conf/nexmark.yaml.template" "$NEXMARK_HOME/conf/nexmark.yaml" \
    METRIC_PORT="$METRIC_PORT" MONITOR_DELAY="$MONITOR_DELAY" MONITOR_INTERVAL="$MONITOR_INTERVAL" \
    EVENTS_NUM="$EVENTS_NUM" TPS="$TPS" QUERIES="$QUERIES" WARMUP_DURATION="$WARMUP_DURATION" \
    WARMUP_EVENTS_NUM="$WARMUP_EVENTS_NUM" REST_PORT="$REST_PORT"

  if [ "$mode" = "gluten" ]; then
    mkdir -p "$FLINK_HOME/gluten_lib"
    cp "$BUNDLE_JAR" "$FLINK_HOME/gluten_lib/"
    # Gluten classes must load before Flink's, so prepend the bundle to the classpath that
    # constructFlinkClassPath builds (see gluten-flink/docs/Flink.md).
    python3 -I - "$FLINK_HOME/bin/config.sh" "$FLINK_HOME/gluten_lib/$(basename "$BUNDLE_JAR")" <<'EOF'
import sys

path, jar = sys.argv[1:]
old = 'echo "$FLINK_CLASSPATH""$FLINK_DIST"'
with open(path) as f:
    text = f.read()
if text.count(old) != 1:
    sys.exit("Cannot find the classpath line to patch in " + path)
with open(path, "w") as f:
    f.write(text.replace(old, 'echo "%s:$FLINK_CLASSPATH""$FLINK_DIST"' % jar))
EOF
  fi
  export FLINK_HOME NEXMARK_HOME
}

# --- Cluster -----------------------------------------------------------------------------------

CLUSTER_UP=""

start_cluster() {
  if rest /overview > /dev/null 2>&1; then
    echo "Something is already serving on port $REST_PORT; stop it or pass --rest_port."
    exit 1
  fi
  log "Starting JobManager and $NUM_TM TaskManager(s) from $FLINK_HOME"
  CLUSTER_UP=1
  "$FLINK_HOME/bin/jobmanager.sh" start > /dev/null
  local i
  for i in $(seq 1 "$NUM_TM"); do
    "$FLINK_HOME/bin/taskmanager.sh" start > /dev/null
  done
  local waited=0 tms=0
  while [ "$waited" -lt 120 ]; do
    tms=$(rest /overview 2> /dev/null | json 'print(d["taskmanagers"])' 2> /dev/null || echo 0)
    [ "$tms" -ge "$NUM_TM" ] && break
    sleep 1
    waited=$((waited + 1))
  done
  if [ "$tms" -lt "$NUM_TM" ]; then
    echo "Only $tms of $NUM_TM TaskManagers registered after 120s, see $FLINK_HOME/log."
    exit 1
  fi
  # The CPU sampler looks up TaskManager pids once at startup, so it must start after them.
  "$NEXMARK_HOME/bin/setup_cluster.sh" > "$NEXMARK_HOME/log/setup_cluster.out" 2>&1
}

stop_cluster() {
  [ -n "$CLUSTER_UP" ] || return 0
  log "Stopping cluster"
  "$NEXMARK_HOME/bin/shutdown_cluster.sh" > /dev/null 2>&1 || true
  "$FLINK_HOME/bin/taskmanager.sh" stop-all > /dev/null 2>&1 || true
  "$FLINK_HOME/bin/jobmanager.sh" stop-all > /dev/null 2>&1 || true
  CLUSTER_UP=""
}

trap stop_cluster EXIT

# --- Benchmark ---------------------------------------------------------------------------------

run_mode() {
  local mode=$1
  local out="$RESULT_DIR/$mode"
  mkdir -p "$out/jobs"
  : > "$out/status.tsv"
  setup_mode "$mode"
  start_cluster

  local q rc status before id n
  for q in ${QUERIES//,/ }; do
    log "[$mode] Running $q"
    before=$(job_ids)
    set +e
    timeout "$QUERY_TIMEOUT" "$NEXMARK_HOME/bin/run_query.sh" oa "$q" > "$out/$q.log" 2>&1
    rc=$?
    set -e
    case $rc in
      0) status=ok ;;
      124) status=timeout ;;
      *) status=failed ;;
    esac
    if [ "$status" = "ok" ] && ! grep -q "^|$q " "$out/$q.log"; then
      status=failed
    fi
    log "[$mode] $q: $status"
    printf '%s\t%s\t%s\n' "$q" "$status" "$rc" >> "$out/status.tsv"
    cancel_running_jobs
    # Save the job details (including the plan) of this query's jobs, to check what ran natively.
    n=0
    for id in $(job_ids); do
      if ! grep -qx "$id" <<< "$before"; then
        n=$((n + 1))
        rest "/jobs/$id" > "$out/jobs/$q-$n-$id.json" || true
      fi
    done
  done

  stop_cluster
  cp -R "$FLINK_HOME/log" "$out/flink-log"
  cp -R "$NEXMARK_HOME/log" "$out/nexmark-log"
}

if [ "$(uname -s)" != "Linux" ]; then
  echo "Nexmark's CPU sampler reads /proc and velox4j ships Linux libraries only; run this on Linux."
  exit 1
fi
for tool in curl python3 java git timeout; do
  command -v "$tool" > /dev/null || { echo "$tool is required."; exit 1; }
done

prepare_flink
prepare_nexmark
case ",$MODES," in *,gluten,*) find_bundle ;; esac

cat > "$RESULT_DIR/run.env" <<EOF
flink_version=$FLINK_VERSION
nexmark_commit=$NEXMARK_COMMIT
gluten_commit=$(git -C "$GLUTEN_DIR" rev-parse --short HEAD 2> /dev/null || echo unknown)
bundle=${BUNDLE_JAR:-}
host=$(hostname)
cpus=$(nproc 2> /dev/null || echo unknown)
java=$(java -version 2>&1 | head -1)
modes=$MODES
queries=$QUERIES
events_num=$EVENTS_NUM
tps=$TPS
warmup_events_num=$WARMUP_EVENTS_NUM
num_tm=$NUM_TM
slots=$SLOTS
parallelism=$PARALLELISM
tm_memory=$TM_MEMORY
state_backend=$STATE_BACKEND
EOF

for mode in ${MODES//,/ }; do
  case $mode in
    vanilla|gluten) run_mode "$mode" ;;
    *) echo "Unknown mode: $mode"; exit 1 ;;
  esac
done

python3 -I "$BENCH_DIR/report.py" "$RESULT_DIR"
log "Results in $RESULT_DIR"
