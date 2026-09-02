# Spark Log Analysis

A practical Apache Spark project built with Scala to analyze web application log data. This project demonstrates Spark DataFrames, Spark SQL, Broadcast Variables, Accumulators, data-quality validation, CSV output generation, and Spark Streaming with DStreams.

## Project Overview

The application reads web application logs, converts raw records into a typed Spark DataFrame, validates data quality, performs analytical queries, and produces organized results.

### Input Fields

- `timestamp` — request timestamp
- `level` — log level such as INFO or ERROR
- `ip` — client IP address
- `url` — requested endpoint
- `status` — HTTP status code
- `response_time` — response time in milliseconds

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
│   │       ├── StatelessWordCount.scala
│   │       ├── StatefulWordCount.scala
│   │       ├── StatefulErrorCounter.scala
│   │       └── streaming/
│   │           └── LogStreaming.scala
│   └── test/
│       └── scala/
│           └── LogAnalysisSpec.scala
│
├── .gitignore
├── build.sbt
└── README.md
```

## Spark Features

### DataFrames
Structured log processing, filtering, grouping, aggregation, sorting, and CSV generation.

### Spark SQL
The processed DataFrame is registered as a temporary view for SQL-based log analysis.

### Broadcast Variables
A slow-request threshold is shared with Spark executors through a broadcast variable.

### Accumulators
Counters are used for total, successful, error, and slow requests.

### Spark Streaming
The project includes stateless and stateful DStream examples using a TCP socket on `localhost:9999` with 5-second micro-batches.

## Analyses

1. Total request count
2. Log-level analysis
3. HTTP status analysis
4. Error-log analysis
5. Unique IP analysis
6. URL request analysis
7. URL and status analysis
8. Response-time statistics
9. Response time by URL
10. Slow-request analysis
11. IP-wise request analysis
12. IP-wise error analysis
13. Top requesting IP
14. IP with most errors
15. Spark SQL analysis
16. Data quality validation
17. Accumulator summary

## Technologies

- Scala 2.12
- Apache Spark 3.5.6
- Spark SQL
- Spark Streaming
- SBT
- Java 17
- Linux / WSL2
- Git and GitHub

## How to Run

```bash
cd ~/spark-log-analysis
sbt compile
sbt run
```

Save application output:

```bash
mkdir -p output
sbt run > output/spark-log-analysis-output.txt 2>&1
```

Run tests:

```bash
sbt test
```

## Spark Streaming

Start a TCP producer:

```bash
nc -lk 9999
```

Then run one application in another terminal:

```bash
sbt "runMain StatelessWordCount"
```

```bash
sbt "runMain StatefulWordCount"
```

```bash
sbt "runMain StatefulErrorCounter"
```

For the streaming log analyzer:

```bash
sbt "runMain LogStreaming"
```

Stateful applications use checkpointing to maintain state across micro-batches. Runtime checkpoint data is excluded from Git.

## Testing

`LogAnalysisSpec.scala` verifies record counts, successful and error requests, unique IPs, slow requests, duplicate records, null values, HTTP status validity, response-time validity, and overall data-quality validation.

## Note

Generated Spark output directories can contain multiple part files when Spark writes distributed output. The repository structure above represents the organized result files intended for project documentation and practice.
