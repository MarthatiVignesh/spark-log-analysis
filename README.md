# Spark Log Analysis

A practical Apache Spark + Scala project combining web-log analysis with aggregations, joins, window functions, execution-plan analysis, and Spark Streaming.

## Technology Stack

- Scala 2.12.18
- Apache Spark 3.5.6
- Spark Core
- Spark SQL
- Spark Streaming
- sbt 2.0.7
- Java 17+
- Ubuntu / WSL2

## Project Structure

```text
spark-log-analysis/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── data/
│   └── application.log
│
├── docs/
│   ├── execution-plan.md
│   └── terminal-output.svg
│
├── output/
│   ├── error-logs/
│   │   └── error-logs.csv
│   ├── ip-analysis/
│   │   └── ip-analysis.csv
│   ├── processed-logs/
│   │   └── processed-logs.csv
│   ├── slow-requests/
│   │   └── slow-requests.csv
│   ├── status-analysis/
│   │   └── status-analysis.csv
│   ├── url-analysis/
│   │   └── url-analysis.csv
│   └── streaming/
│       ├── streaming-log-analysis-output.txt
│       ├── stateless/
│       │   └── stateless-word-count-output.txt
│       └── stateful/
│           ├── stateful-word-count-output.txt
│           └── stateful-error-counter-output.txt
│
├── project/
│   └── build.properties
│
├── src/
│   ├── main/
│   │   └── scala/
│   │       ├── LogAnalysis.scala
│   │       ├── AggregationsJoinsWindows.scala
│   │       ├── StatelessWordCount.scala
│   │       ├── StatefulWordCount.scala
│   │       ├── StatefulErrorCounter.scala
│   │       └── streaming/
│   │           └── LogStreaming.scala
│   └── test/
│       └── scala/
│           └── LogAnalysisSpec.scala
│
├── OUTPUT.md
├── .gitignore
├── build.sbt
└── README.md
```

## 1. Log Analysis

`LogAnalysis.scala` processes web application logs using Spark DataFrames and Spark SQL. It demonstrates filtering, grouping, aggregation, CSV output, broadcast variables, accumulators, and data-quality validation.

Analyses include request counts, HTTP status analysis, error logs, unique IPs, URL analysis, response-time statistics, slow requests, IP-wise analysis, and Spark SQL queries.

## 2. Aggregations, Joins and Windows

`AggregationsJoinsWindows.scala` demonstrates:

- Simple `sum`, `avg`, `min`, `max`, and `count` aggregations
- Product and customer grouping
- Running totals using Window functions
- Customer order ranking using `row_number()`
- Inner joins using `customer_id`
- Aggregation after joins
- Extended Spark execution plans with `explain(true)`
- Shuffle, Exchange, Sort, and broadcast-join concepts

The documented results are available in `OUTPUT.md`.

## 3. Spark Streaming

The project includes stateless and stateful DStream examples using TCP socket input on `localhost:9999` with 5-second micro-batches.

```bash
nc -lk 9999
```

Run an example in another terminal:

```bash
sbt "runMain StatelessWordCount"
sbt "runMain StatefulWordCount"
sbt "runMain StatefulErrorCounter"
sbt "runMain LogStreaming"
```

Stateful applications use checkpointing to maintain state across micro-batches.

## How to Run

```bash
cd ~/spark-log-analysis
sbt compile
```

Run the main log-analysis application:

```bash
sbt run
```

Run the aggregations/joins/windows exercise:

```bash
sbt "runMain AggregationsJoinsWindows"
```

Run tests:

```bash
sbt test
```

## Documentation

- `OUTPUT.md` — documented results for the aggregations/joins/windows exercise.
- `docs/execution-plan.md` — explanation of Spark logical and physical execution plans.
- `docs/terminal-output.svg` — terminal-output visual.

## Testing

`LogAnalysisSpec.scala` verifies record counts, successful and error requests, unique IPs, slow requests, duplicate records, null values, HTTP status validity, response-time validity, and overall data-quality validation.

## GitHub Actions

`.github/workflows/ci.yml` runs compilation and tests automatically for pushes and pull requests targeting `main`.
