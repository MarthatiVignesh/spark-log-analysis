import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class LogAnalysisSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Spark Log Analysis Tests")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  private def loadLogs() = {
    val rawDF = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .csv("data/application.log")

    rawDF.toDF("timestamp", "level", "ip", "url", "status", "response_time")
      .withColumn("status", col("status").cast("int"))
      .withColumn("response_time", col("response_time").cast("int"))
  }

  test("Input log file should contain 10 records") {
    assert(loadLogs().count() == 10)
  }

  test("There should be 7 successful requests") {
    assert(loadLogs().filter(col("status") >= 200 && col("status") < 400).count() == 7)
  }

  test("There should be 3 error requests") {
    assert(loadLogs().filter(col("level") === "ERROR").count() == 3)
  }

  test("There should be 5 unique IP addresses") {
    assert(loadLogs().select("ip").distinct().count() == 5)
  }

  test("There should be 3 slow requests") {
    assert(loadLogs().filter(col("response_time") >= 400).count() == 3)
  }

  test("There should be no duplicate records") {
    val logs = loadLogs()
    assert(logs.count() == logs.distinct().count())
  }

  test("There should be no null values in required columns") {
    val logs = loadLogs()
    val nullRecords = logs.filter(
      col("timestamp").isNull || col("level").isNull || col("ip").isNull ||
      col("url").isNull || col("status").isNull || col("response_time").isNull
    ).count()
    assert(nullRecords == 0)
  }

  test("All HTTP status codes should be valid") {
    assert(loadLogs().filter(col("status") < 100 || col("status") > 599).count() == 0)
  }

  test("All response times should be non-negative") {
    assert(loadLogs().filter(col("response_time") < 0).count() == 0)
  }

  test("All 10 records should pass data quality validation") {
    val logs = loadLogs()
    val validRecords = logs.filter(
      col("timestamp").isNotNull && col("level").isNotNull && col("ip").isNotNull &&
      col("url").isNotNull && col("status").isNotNull && col("status") >= 100 &&
      col("status") <= 599 && col("response_time").isNotNull && col("response_time") >= 0
    ).count()
    assert(validRecords == 10)
  }
}
