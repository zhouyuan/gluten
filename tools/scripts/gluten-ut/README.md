<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Cutting the gluten-ut boilerplate (prototype, Spark 3.5 + Velox)

Running Spark's own test suites under Gluten costs two kinds of hand-written code,
duplicated for every supported Spark version:

1. **Extending** — one subclass per suite, `class GlutenXSuite extends XSuite with
   GlutenSQLTestsBaseTrait {}`, plus one `enableSuite[...]` line and an exclusion list
   in a per-backend `*TestSettings.scala`.
2. **Porting** — copying a Spark test body into gluten-ut and editing it.

Before this prototype, `gluten-ut/spark35/src/test/scala` held 269 files; 165 of the
319 classes in it were empty subclasses, and `VeloxTestSettings.scala` was 988 lines.
Across spark34/35/40/41, 168 of the 242 files present in all four versions are
byte-identical.

## What is here

**1. The subclasses become a data file.**
`spark35/src/test/resources/gluten-ut/suites.txt` lists one line per suite:

```
org.apache.spark.sql.CTEHintSuite | org.apache.spark.sql.GlutenSQLTestsTrait
```

`ut_suites.py generate` turns that into the subclasses under
`target/generated-test-sources/gluten-ut` during `generate-test-sources` (wired into
`spark35/pom.xml`). Nothing is generated into `src/`, so there is nothing to commit or
review. Generated code always names the base class fully qualified, so a wrong entry is
a compile error rather than a silently mis-wired suite.

**2. The settings become a data file.**
`spark35/src/test/resources/gluten-ut/settings-velox.txt` holds one directive per line:

```
enable  org.apache.spark.sql.connector.GlutenDataSourceV2Suite
# Rewrite the following tests in GlutenDataSourceV2Suite.
exclude org.apache.spark.sql.connector.GlutenDataSourceV2Suite#partitioning reporting
```

`org.apache.gluten.utils.DataFileBackendTestSettings` reads it at run time and drives the
existing `BackendTestSettings` DSL, so the semantics (include / exclude / by-prefix /
`Gluten - ` prefixed tests) are unchanged. `VeloxTestSettings.scala` is now 27 lines.
Every enabled suite is resolved with `Class.forName` at load time, so a typo fails fast
instead of quietly skipping a suite.

This is the shape the Delta suite already uses successfully
(`.github/workflows/util/delta-spark-ut/known-failures.txt`, 728 entries, gated by
`compare-test-results.py`): expected failures as reviewable data, not code. Once the
settings are data, that gate can be pointed at gluten-ut too, and a Spark version bump
becomes a diff in a text file instead of a few hundred new Scala files.

**3. Plan assertions that only differ by `Transformer`.**
Many ports exist because the test asserts on the operator type and, after offload, a
`SortExec` is a `SortExecTransformer`. `GlutenVanillaPlanView` (in gluten-ut/common)
overrides the `AdaptiveSparkPlanHelper` helpers Spark's tests use -- `collect`, `find`,
`collectFirst`, `collectWithSubqueries` -- so a predicate matches an offloaded operator
wherever it would match the vanilla one. It is additive: assertions on `*Transformer`
types keep working, and unrelated operator types still do not match. Only same-operator
pairs belong in the mapping; a test that wants `SortMergeJoinExec` where Gluten plans a
shuffled hash join is asserting something genuinely no longer true and still needs an
exclusion or a rewrite.

## Using the tool

```bash
# One-shot migration of a Spark version / backend, with a report of anything ambiguous.
python3 tools/scripts/gluten-ut/ut_suites.py extract --module gluten-ut/spark35 --backends velox

# Confirm the catalog reproduces every hand-written subclass, then delete them.
python3 tools/scripts/gluten-ut/ut_suites.py verify --module gluten-ut/spark35 \
    --catalog gluten-ut/spark35/src/test/resources/gluten-ut/suites.txt
python3 tools/scripts/gluten-ut/ut_suites.py prune  --module gluten-ut/spark35 \
    --catalog gluten-ut/spark35/src/test/resources/gluten-ut/suites.txt --apply
```

`extract` is idempotent: re-running keeps entries whose sources have already been pruned,
and keeps a base package that was corrected by hand.

## How the migration is verified

- `verify` proves the catalog reproduces all 165 subclasses, byte for byte in declaration.
- `extract` cross-checks that every DSL call site in the original Scala was consumed.
- `VeloxSettingsMigrationSuite` (spark35) constructs both the old `VeloxTestSettingsLegacy`
  and the new data-driven `VeloxTestSettings` and asserts they enable the same suites and
  make the same run/skip decision for every test name the settings mention. It needs no
  SparkSession and no native library. This caught a real extractor bug: test names written
  as `"SPARK-19471: ..." + " before using it"` were being split in two.
- `GlutenVanillaPlanViewInterceptionSuite` (gluten-ut/common) proves the helper overrides
  redirect plan assertions, with a stubbed mapping so it needs no backend.
- `GlutenVanillaPlanViewSuite` (spark35) covers the real operator mapping; it needs a
  backend jar on the classpath.

Delete `VeloxTestSettingsLegacy.scala` and `VeloxSettingsMigrationSuite.scala` once the
migration is accepted -- they exist only to prove equivalence.

## Scope and caveats

- Only spark35 + Velox is migrated. The ClickHouse and Bolt settings for spark35 are
  untouched and still work, because the generated subclasses exist for every backend.
- Build-time generation needs `python3` on `PATH`.
- 5 subclasses are still hand-written: they are declared in a file that also holds other
  code, so `prune` leaves them alone.
- The operator mapping covers Sort, Project, Filter and Window. Scans, joins and
  aggregates are the frequent remaining cases, and their vanilla constructors differ more
  between Spark versions -- worth doing next, with a per-version shim if needed.
- Not yet run end to end against a live Gluten plan: that needs the native library, i.e.
  CI.
