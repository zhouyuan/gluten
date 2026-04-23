# Multi-Grouping-Set Hash Aggregation Optimization

## Overview

This optimization improves the performance of GROUPING SETS, ROLLUP, and CUBE operations in TPC-DS queries (like Q67) by replacing the standard Expand-then-Aggregate pattern with Velox's `MultiGroupingSetHashAggregation`, which uses a shared hash table across multiple grouping sets.

## Problem Statement

In standard Spark execution, GROUPING SETS/ROLLUP/CUBE operations are implemented using:
1. **Expand**: Duplicates input rows for each grouping set, adding a grouping ID column
2. **Aggregate**: Performs aggregation on the expanded data

For example, in TPC-DS Q67 with 8 grouping columns and ROLLUP, this creates multiple copies of the data (one per grouping set), leading to:
- High memory usage due to data duplication
- Suboptimal performance as each grouping set is processed independently
- Multiple hash table builds for similar grouping patterns

## Solution

The `MultiGroupingSetHashAggregation` optimization:
- Detects the Expand-Aggregate pattern during query planning
- Replaces it with a single aggregation operator that processes all grouping sets in one pass
- Uses a **shared hash table** across grouping sets, significantly reducing memory footprint
- Eliminates the need for data duplication from the Expand operator

## Architecture

### Components

1. **MultiGroupingSetHashAggregateExecTransformer** (`backends-velox/src/main/scala/org/apache/gluten/execution/MultiGroupingSetHashAggregateExecTransformer.scala`)
   - New aggregation transformer that extends `HashAggregateExecTransformer`
   - Carries grouping set information to the Velox backend
   - Generates Substrait plans with multi-grouping-set semantics

2. **MultiGroupingSetAggregateRule** (`backends-velox/src/main/scala/org/apache/gluten/extension/MultiGroupingSetAggregateRule.scala`)
   - Optimization rule that detects Expand-Aggregate patterns
   - Extracts grouping set information from Expand projections
   - Validates the pattern is suitable for optimization
   - Transforms the plan to use `MultiGroupingSetHashAggregateExecTransformer`

3. **Configuration** (`backends-velox/src/main/scala/org/apache/gluten/config/VeloxConfig.scala`)
   - New config option: `spark.gluten.sql.columnar.backend.velox.multiGroupingSetHashAggregation`
   - Default: `true` (enabled)

### Rule Registration

The rule is registered in `VeloxRuleApi` as a post-transform rule, executed before `FlushableHashAggregateRule` to ensure proper optimization order.

## Usage

### Enable/Disable the Optimization

```scala
// Enable (default)
spark.conf.set("spark.gluten.sql.columnar.backend.velox.multiGroupingSetHashAggregation", "true")

// Disable
spark.conf.set("spark.gluten.sql.columnar.backend.velox.multiGroupingSetHashAggregation", "false")
```

### Example Query

```sql
-- TPC-DS Q67 style query with ROLLUP
SELECT 
  i_category,
  i_class,
  i_brand,
  i_product_name,
  d_year,
  d_qoy,
  d_moy,
  s_store_id,
  SUM(ss_sales_price) as sum_sales,
  COUNT(*) as cnt
FROM store_sales
JOIN item ON ss_item_sk = i_item_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN store ON ss_store_sk = s_store_sk
GROUP BY ROLLUP(
  i_category,
  i_class,
  i_brand,
  i_product_name,
  d_year,
  d_qoy,
  d_moy,
  s_store_id
)
```

**Before optimization:**
```
HashAggregate
  └── Expand (creates 2^8 = 256 grouping sets)
        └── Join/Scan
```

**After optimization:**
```
MultiGroupingSetHashAggregate (shared hash table for all 256 grouping sets)
  └── Join/Scan
```

## Validation Criteria

The optimization is applied when:
1. An aggregate has an Expand child
2. At least 2 grouping sets are present (otherwise no benefit from shared hash table)
3. Aggregate expressions are in Partial or Complete mode
4. The Expand output structure matches expected pattern

## Performance Benefits

- **Memory Reduction**: Eliminates data duplication from Expand operator
- **Computation Efficiency**: Single-pass aggregation with shared hash table
- **Cache Efficiency**: Better CPU cache utilization due to reduced data movement
- **Scalability**: Particularly beneficial for queries with many grouping sets (like TPC-DS Q67)

## Implementation Notes

### Grouping Set Extraction

The rule analyzes Expand projections to identify which grouping expressions are active in each set:
- `Literal(null, _)` indicates an inactive grouping expression
- Other expressions indicate active grouping expressions
- Each projection represents one grouping set

### Compatibility

- Works with both `RegularHashAggregateExecTransformer` and `HashAggregateExecTransformer`
- Compatible with other Velox optimizations (e.g., FlushableHashAggregation)
- Requires Velox backend with MultiGroupingSetHashAggregation support

## Future Enhancements

1. **Substrait Plan Generation**: Full implementation of grouping sets in Substrait plan
2. **Cost-Based Optimization**: Decide when to apply based on estimated grouping set count
3. **Partial Aggregation Support**: Extend to PartialMerge and Final modes
4. **Dynamic Grouping Sets**: Support for runtime-determined grouping sets

## Testing

To verify the optimization is working:

```scala
// Run a query with GROUPING SETS/ROLLUP/CUBE
val df = spark.sql("""
  SELECT col1, col2, SUM(col3)
  FROM table
  GROUP BY ROLLUP(col1, col2)
""")

// Check the physical plan
df.explain(true)

// Look for "MultiGroupingSetHashAggregate" in the plan
// If present, optimization is applied
```

## References

- Velox MultiGroupingSetHashAggregation: [Velox Documentation]
- TPC-DS Q67: Standard benchmark query using ROLLUP with 8 columns
- Spark GROUPING SETS: [Spark SQL Documentation]