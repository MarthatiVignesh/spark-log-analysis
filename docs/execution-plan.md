# Spark Execution Plan

This document explains how Spark builds an execution plan for the aggregations, joins, and window operations in `AggregationsJoinsWindows.scala`.

## Logical Plan

Spark first represents DataFrame transformations as a logical plan. Operations such as `select`, `groupBy`, `join`, and `withColumn` are analyzed before execution.

## Physical Plan

Spark then chooses an executable physical plan. For small in-memory customer/order DataFrames, a broadcast join may be selected. Grouping and window operations can require `Exchange` and `Sort` stages.

Typical stages include:

```text
Scan / LocalRelation
  -> Join
  -> Exchange
  -> Sort
  -> Window
  -> Project / Sort
```

## Inspecting the Plan

The project uses:

```scala
joinedData.explain(true)
finalResult.explain(true)
```

`explain(true)` displays parsed, analyzed, optimized logical plans and the physical plan.

## Key Concepts

- **Exchange**: redistributes data between partitions.
- **Sort**: orders records for operations that require a defined ordering.
- **BroadcastHashJoin**: can avoid a large shuffle when one side is small enough to broadcast.
- **Window**: computes values across related rows while preserving row-level detail.
