#!/usr/bin/env python3
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

"""Builds results.csv and summary.md from a run.sh result directory.

Usage: report.py <result_dir>
"""

import csv
import math
import os
import sys

MODES = ("vanilla", "gluten")


def parse_float(text):
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return math.nan


def parse_result_row(log_path, query):
    """Returns the query's row of the "Nexmark Results" table that the harness prints."""
    with open(log_path, errors="replace") as f:
        for line in f:
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if line.startswith("|") and len(cells) >= 5 and cells[0] == query:
                events = parse_float(cells[1])
                time_s = parse_float(cells[3])
                return {
                    "events_num": int(events) if not math.isnan(events) else "",
                    "cores": parse_float(cells[2]),
                    "time_s": time_s,
                    "cores_time": parse_float(cells[4]),
                    "throughput_eps": events / time_s if time_s > 0 else math.nan,
                }
    return None


def load_mode(result_dir, mode):
    status_path = os.path.join(result_dir, mode, "status.tsv")
    if not os.path.exists(status_path):
        return None
    rows = {}
    with open(status_path) as f:
        for line in f:
            query, status, _ = line.rstrip("\n").split("\t")
            row = {"status": status}
            if status == "ok":
                parsed = parse_result_row(os.path.join(result_dir, mode, query + ".log"), query)
                if parsed:
                    row.update(parsed)
                else:
                    row["status"] = "no result"
            rows[query] = row
    return rows


def fmt(value, digits=2):
    if value is None or value == "" or (isinstance(value, float) and math.isnan(value)):
        return "-"
    return "%.*f" % (digits, value)


def load_env(result_dir):
    env = {}
    path = os.path.join(result_dir, "run.env")
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                if "=" in line:
                    key, value = line.rstrip("\n").split("=", 1)
                    env[key] = value
    return env


def main():
    result_dir = sys.argv[1]
    results = {m: load_mode(result_dir, m) for m in MODES}
    results = {m: r for m, r in results.items() if r is not None}
    if not results:
        sys.exit("No results found in " + result_dir)

    queries = []
    for rows in results.values():
        queries += [q for q in rows if q not in queries]

    fields = ["mode", "query", "status", "events_num", "cores", "time_s", "cores_time",
              "throughput_eps"]
    with open(os.path.join(result_dir, "results.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, restval="")
        writer.writeheader()
        for mode, rows in results.items():
            for query, row in rows.items():
                writer.writerow(dict(row, mode=mode, query=query))

    env = load_env(result_dir)
    lines = ["# Nexmark results", ""]
    lines += ["- %s: %s" % (k, v) for k, v in env.items()]
    lines.append("")

    both = len(results) == 2
    header = ["Query"]
    for mode in results:
        header += ["%s time (s)" % mode, "%s cores" % mode, "%s events/s" % mode]
    if both:
        header.append("Speedup")
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|" + "---|" * len(header))

    totals = {m: 0.0 for m in results}
    compared = 0
    for query in queries:
        cells = [query]
        times = {}
        for mode, rows in results.items():
            row = rows.get(query, {"status": "not run"})
            if row["status"] == "ok":
                times[mode] = row["time_s"]
                cells += [fmt(row["time_s"], 3), fmt(row["cores"]),
                          fmt(row["throughput_eps"], 0)]
            else:
                cells += [row["status"], "-", "-"]
        if both:
            if len(times) == 2 and times["gluten"] > 0:
                cells.append(fmt(times["vanilla"] / times["gluten"]) + "x")
                for mode in results:
                    totals[mode] += times[mode]
                compared += 1
            else:
                cells.append("-")
        lines.append("| " + " | ".join(cells) + " |")

    if both and compared:
        lines.append("")
        lines.append("Total time over the %d %s that succeeded in both modes: "
                     "vanilla %.3f s, gluten %.3f s, speedup %.2fx." % (
                         compared, "query" if compared == 1 else "queries",
                         totals["vanilla"], totals["gluten"],
                         totals["vanilla"] / totals["gluten"]))
    lines.append("")
    lines.append("Time is from the job reaching RUNNING until all events are processed. "
                 "Cores is the average CPU of the TaskManager processes, sampled from /proc. "
                 "Check <mode>/jobs/*.json for each query's plan, to see whether the gluten run "
                 "executed natively or fell back.")

    summary = "\n".join(lines) + "\n"
    with open(os.path.join(result_dir, "summary.md"), "w") as f:
        f.write(summary)
    print(summary)


if __name__ == "__main__":
    main()
