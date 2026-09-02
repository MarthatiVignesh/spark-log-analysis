# Verified Output

This file documents the expected output for the Spark Aggregations, Joins and Windows exercise.

## Environment

- Scala: 2.12.18
- Apache Spark: 3.5.6
- Java: 17+
- sbt: 2.0.7
- OS: Ubuntu / WSL2
- Command: `sbt run`

## Example 1 — Sales Analysis

### Simple Aggregation

```text
Total sales:       300000
Average sale:      37500.0
Minimum sale:     15000
Maximum sale:     60000
Number of orders: 8
```

### Product Aggregation

```text
Laptop -> Total: 210000, Average: 52500.0, Orders: 4
Mobile -> Total:  90000, Average: 22500.0, Orders: 4
```

### Customer Aggregation

```text
Amit    -> 60000, 2 orders
Chintan -> 75000, 2 orders
Priya   -> 90000, 2 orders
Rahul   -> 75000, 2 orders
```

### Window Operations

The program prints a customer-partitioned running total and ranks each customer's orders by amount in descending order.

## Example 2 — JOIN

The customer and order DataFrames are joined on `customer_id`.

```text
Chintan -> 75000 from 2 orders
Rahul   -> 60000 from 2 orders
Priya   -> 30000 from 1 order
Amit    -> 15000 from 1 order
```

The application also prints the extended execution plan with `explain(true)`.

## Final Exercise — JOIN + GROUPING + WINDOW

```text
customer_id  customer_name  city       order_id  amount  total_spending  order_rank
1            Chintan        Hyderabad  101       50000   75000           1
1            Chintan        Hyderabad  103       25000   75000           2
2            Rahul          Mumbai     106       40000   60000           1
2            Rahul          Mumbai     102       20000   60000           2
3            Priya          Bangalore  104       30000   30000           1
4            Amit           Delhi      105       15000   15000           1
```

> This file is documentation for the exercise. Run `sbt compile` and `sbt run` in Ubuntu/WSL2 to produce the live console output for your environment.
