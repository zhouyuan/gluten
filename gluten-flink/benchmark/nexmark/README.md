# Nexmark benchmark for Gluten Flink

Runs the [Nexmark](https://github.com/nexmark/nexmark) streaming benchmark on a local standalone
Flink cluster, once with vanilla Flink and once with the Gluten bundle, and compares the two.

`run.sh` does everything:

1. Downloads the Flink binary that gluten-flink targets (`flink.version` in `gluten-flink/pom.xml`)
   and checks its SHA-512.
2. Clones Nexmark at a pinned commit and builds it against that Flink version.
3. For each mode, installs a fresh Flink and Nexmark under `work/<mode>/`. The gluten install gets
   the bundle jar in `gluten_lib/`, with `bin/config.sh` patched to put it first on the classpath.
4. Starts a JobManager and `--num_tm` TaskManagers on localhost, plus Nexmark's CPU sampler.
5. Runs each query in its own Nexmark harness process, so one failing query does not stop the
   rest, then stops the cluster.
6. Writes `results.csv` and `summary.md` with `report.py`.

## Requirements

- Linux on x86_64. velox4j ships Linux native libraries only, and Nexmark's CPU sampler reads
  `/proc`.
- JDK 11 or 17, plus `curl`, `git`, `python3` and `timeout`.
- The Gluten bundle jar, built with `gluten-flink/dev/package.sh`.
- Network access to GitHub, Maven Central and the Apache download servers for the first run.
  Flink is fetched from `dlcdn.apache.org` when the CDN still hosts that version, otherwise from
  the much slower `archive.apache.org`. Set `FLINK_URL` to use another mirror, or pass
  `--flink_tgz` to use a tarball you already have. Downloads are checked against the archive's
  SHA-512 file.

## Usage

```bash
./gluten-flink/dev/package.sh
./gluten-flink/benchmark/nexmark/run.sh --queries=q0,q1,q2 --events_num=100000000
```

`run.sh --help` lists all options. The main ones:

| Option | Default | Meaning |
|---|---|---|
| `--modes` | `vanilla,gluten` | Which sides to run. |
| `--queries` | all supported | q6, q13 and q23 are left out because gluten-flink does not support them yet. |
| `--events_num` | `10000000` | Events per query. Size it so each query runs well past the 3s monitor delay; shorter jobs produce no metrics and fail. |
| `--tps` | `10000000` | Source rate limit. Keep it above what the cluster can process to measure peak throughput. |
| `--warmup_events_num` | `0` | Warmup events before each query, with `--warmup_duration` as a time cap. |
| `--num_tm`, `--slots`, `--parallelism` | `1`, `1`, `1` | Mini cluster shape. |
| `--tm_memory` | `4g` | TaskManager process size. Velox allocates native memory outside this budget, so leave headroom on the host. |
| `--state_backend` | `hashmap` | `rocksdb` also needs FRocksDB for Gluten, see `gluten-flink/docs/Flink.md`. |

To change other Flink settings for both sides, edit `conf/config.yaml.template`.

## Output

Each run writes to `results/<timestamp>/` (or `--result_dir`):

- `summary.md`: per-query time, CPU cores and throughput for each mode, and the speedup.
- `results.csv`: the same data for further analysis.
- `run.env`: versions, commits and settings used.
- `<mode>/<query>.log`: Nexmark harness output.
- `<mode>/jobs/*.json`: the Flink job details, including the plan, for every job a query ran.
  Use these to check whether a gluten query ran natively or fell back to Flink operators.
- `<mode>/flink-log/`, `<mode>/nexmark-log/`: cluster and harness logs.

Time is measured from the job reaching RUNNING until the source has emitted all events and the
job finishes. Throughput is `events_num / time`. Cores is the average CPU used by the TaskManager
processes, including native threads.
