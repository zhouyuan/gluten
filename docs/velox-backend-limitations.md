---
layout: page
title: Velox Backend Limitations
nav_order: 5
---
This document describes the limitations of velox backend by listing some known cases where exception will be thrown, gluten behaves incompatibly with spark, or certain plan's execution
must fall back to vanilla spark, etc.

### Override of Spark classes
Gluten avoids modifying Spark's existing code and prefers Spark APIs when possible. However, some APIs are not exposed by vanilla Spark, so we have to copy the Spark file and apply hardcoded changes. The list of overridden classes can be found as `ignoreClasses` in `package/pom.xml`. If you use a customized Spark, check whether these files are modified in your Spark distribution, otherwise your changes will be overridden.

So you need to ensure preferentially load the Gluten jar to overwrite the jar of vanilla spark. Refer to [How to prioritize loading Gluten jars in Spark](https://github.com/apache/gluten/blob/main/docs/velox-backend-troubleshooting.md#incompatible-class-error-when-using-native-writer).

If an unofficially supported Spark version is used, NoSuchMethodError can be thrown at runtime. More details see [issue-4514](https://github.com/apache/gluten/issues/4514).

### Fallbacks
Except the unsupported operators, functions, file formats, data sources listed in , there are some known cases also fall back to Vanilla Spark. 

#### ANSI
Gluten currently doesn't support ANSI mode. If ANSI is enabled, Spark plan's execution will always fall back to vanilla Spark.
We now have a issue tracker on ANSI support progress. Please check [issue-10134](https://github.com/apache/gluten/issues/10134).

#### Case Sensitive mode
Gluten only supports spark default case-insensitive mode. If case-sensitive mode is enabled, user may get incorrect result.

#### Regexp functions
Spark's regexp functions (`rlike`, `regexp_extract`, `regexp_extract_all`, `regexp_replace`, `split`, etc.) are specified in terms of
`java.util.regex`, while Velox evaluates them with RE2. RE2 runs a finite automaton, so it deliberately omits every feature that needs
backtracking, and it defines a few character classes differently.

Gluten bridges most of that difference by rewriting the pattern into RE2 syntax before it is compiled, in the function overlay
(`cpp/velox/operators/functions/overlay/JavaRegexTranslator.h`). The rewrite covers `rlike`, `regexp_extract`, `regexp_extract_all`,
`regexp_replace` and `split`. `like` is not affected, because its pattern is SQL LIKE rather than a regular expression. These constructs
are handled and need no fallback:

* `\s`, `\S` - `java.util.regex` counts `\x0B` (vertical tab) as whitespace, RE2 does not.
* `\v`, `\V` - `java.util.regex` reads `\v` as the vertical whitespace class, RE2 as the vertical tab character.
* `\h`, `\H`, `\R`, `\e`, `\cX`, `\uHHHH` including surrogate pairs, and `java.util.regex`'s POSIX class names
  (`\p{Alpha}`, `\p{Space}`, `\p{ASCII}`, ...) - all unknown to RE2.
* Named capturing groups, which `java.util.regex` spells `(?<name>...)` and RE2 spells `(?P<name>...)`.

These remain unsupported, because RE2 needs backtracking to evaluate them and deliberately does not do it:
* Lookaround (lookahead/lookbehind): `(?=...)`, `(?!...)`, `(?<=...)`, `(?<!...)`
* Backreferences, e.g. `(\d)\1`
* Possessive quantifiers `?+`, `*+`, `++`, `{n}+`, and independent non-capturing groups `(?>...)`
* `\G` and `\Z`

These are unsupported too, because RE2 cannot express them:
* Character class union, intersection and difference: `[a[b]]`, `[a&&[b]]`, `[a&&[^b]]`
* A negated shorthand class nested in a character class, e.g. `[\Sx]`, `[\Hx]`, `[\Vx]` - RE2 cannot nest a complement
* `\p{...}` names that only `java.util.regex` has, e.g. `\p{IsAlphabetic}` and `\p{javaLowerCase}`. The Unicode script and category
  names the two engines share, e.g. `\p{L}` and `\p{Greek}`, do work.

An unsupported pattern is rejected while Gluten validates the native plan, so the expression falls back to vanilla
Spark and the result stays correct. `regexp_replace` and `split` additionally accept a non-constant pattern, which is
compiled per row natively - an unsupported pattern arriving there raises a runtime error instead of falling back.
`regexp_extract_all` with a non-constant pattern compiles it per row inside Velox and is not rewritten, so it still
follows RE2 syntax.

There are a few unknown incompatible cases. If user cannot tolerate the incompatibility risk, please enable the below configuration property.
It falls back `rlike`, `regexp_replace`, `regexp_extract`, `regexp_extract_all` and `split`.
```
spark.gluten.sql.fallbackRegexpExpressions
```

#### FileSource format
Currently, Gluten only fully supports parquet file format and partially support ORC. If other format is used, scan operator falls back to vanilla spark.

#### Partitioned Table Scan
Gluten only support the partitioned table scan when the file path contain the partition info, otherwise will fall back to vanilla spark.

### Incompatible behavior
In certain cases, Gluten result may be different from Vanilla spark.

#### JSON functions
Velox only supports double quotes surrounded strings, not single quotes, in JSON data. If single quotes are used, gluten will produce incorrect result.

Velox doesn't support [*] in path when get_json_object function is called and returns null instead.

#### Parquet read conf
Gluten supports `spark.files.ignoreCorruptFiles` with default false, if true, the behavior is same as config false.
Gluten ignores `spark.sql.parquet.datetimeRebaseModeInRead`, it only returns what write in parquet file. It does not consider the difference between legacy
hybrid (Julian Gregorian) calendar and Proleptic Gregorian calendar. The result may be different with vanilla spark.

#### Parquet write conf
Spark has `spark.sql.parquet.datetimeRebaseModeInWrite` config to decide whether legacy hybrid (Julian + Gregorian) calendar 
or Proleptic Gregorian calendar should be used during parquet writing for dates/timestamps. If the parquet to read is written
by Spark with this config as true, Velox's TableScan will output different result when reading it back.

#### Partition write

Gluten supports static partition writes and dynamic partition writes.

```scala
spark.sql("CREATE TABLE t (c int, d long, e long) STORED AS PARQUET partitioned by (c, d)")
spark.sql("INSERT OVERWRITE TABLE t partition(c=1, d) SELECT 2 as d, 3 as e")
```

Gluten does not support bucket write, and will fall back to vanilla Spark.

```scala
spark.range(100).selectExpr("id as c1", "id % 7 as p")
  .write
  .format("parquet")
  .bucketBy(2, "c1")
  .save(f.getCanonicalPath)
```

#### CTAS write

Gluten supports create table as select with parquet file format.

```scala
spark.range(100).toDF("id")
  .write
  .format("parquet")
  .saveAsTable("velox_ctas")
```

#### HiveFileFormat write

Gluten supports writes of HiveFileFormat when the output file type is of type `parquet` only

#### NaN support

Velox does NOT support NaN. So unexpected result can be obtained for a few cases, e.g., comparing a number with NaN.

#### Configuration

Not all parquet configurations are honored by Gluten. Check docs/velox-parquet-write-configuration.md for details.

### Fetal error caused by Spark's columnar reading

If the user enables Spark's columnar reading, error can occur due to Spark's columnar vector is not compatible with
Gluten's.

### Spill

`OutOfMemoryException` may still be triggered within current implementation of spill-to-disk feature, when shuffle partitions is set to a large number. When this case happens, please try to reduce the partition number to get rid of the OOM.

### Unsupported Data type support in ParquetScan

- Byte type causes fallback to vanilla spark

### Utilizing Map Type as Hash Keys in ColumnarShuffleExchange
Spark's `spark.sql.legacy.allowHashOnMapType` configuration controls whether hashing is allowed on map-type keys.
Gluten enables this configuration when creating `ColumnarShuffleExchange`, as shown [here](https://github.com/apache/gluten/blob/0dacac84d3bf3d2759a5dd7e0735147852d2845d/backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxSparkPlanExecApi.scala#L355-L363).
This bypasses Spark's unresolved-expression checks and lets projects using the `hash(mapType)` operator be created before `ColumnarShuffleExchange`.
However, if `spark.sql.legacy.allowHashOnMapType` is disabled in a test environment, projects using the `hash(mapType)` expression may throw an
`Invalid call to dataType on unresolved object` exception during validation, causing them to fall back to vanilla Spark, as referenced [here](https://github.com/apache/spark/blob/de5fa426e23b84fc3c2bddeabcd2e1eda515abd5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/hash.scala#L291-L296).
Enabling this configuration allows the project to be offloaded to Velox.
