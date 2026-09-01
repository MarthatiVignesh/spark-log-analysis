import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, desc}

object LogAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Log Analysis")
      .master("local[*]")
      .getOrCreate()

    println("Spark application started")

    // Read application log file
    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv("data/application.log")

    // Rename columns
    val logs = df.toDF(
      "timestamp",
      "level",
      "ip",
      "url",
      "status",
      "response_time"
    )

    // Display log data
    println("===== LOG DATA =====")
    logs.show(false)

    // Display schema
    println("===== SCHEMA =====")
    logs.printSchema()

    // Filter ERROR logs
    println("===== ERROR LOGS =====")

    val errorLogs = logs
      .filter("level = 'ERROR'")

    errorLogs.show(false)

    // Count ERROR logs
    println("===== ERROR COUNT =====")

    val errorCount = errorLogs.count()

    println(s"Total ERROR logs: $errorCount")

    // Count logs by level
    println("===== LOG COUNT BY LEVEL =====")

    val levelCounts = logs
      .groupBy("level")
      .count()
      .orderBy("level")

    levelCounts.show(false)

    // Count logs by HTTP status
    println("===== HTTP STATUS COUNT =====")

    val statusCounts = logs
      .groupBy("status")
      .count()
      .orderBy("status")

    statusCounts.show(false)

    // Count requests by URL
    println("===== REQUEST COUNT BY URL =====")

    val urlCounts = logs
      .groupBy("url")
      .count()
      .orderBy(desc("count"))

    urlCounts.show(false)

    // Count requests by IP address
    println("===== REQUEST COUNT BY IP =====")

    val ipCounts = logs
      .groupBy("ip")
      .count()
      .orderBy(desc("count"))

    ipCounts.show(false)

    // Calculate average response time
    println("===== AVERAGE RESPONSE TIME =====")

    val averageResponseTime = logs
      .select(avg("response_time"))
      .first()
      .getDouble(0)

    println(f"Average response time: $averageResponseTime%.2f ms")

    // Find slow requests
    println("===== SLOW REQUESTS (> 400 ms) =====")

    val slowRequests = logs
      .filter("response_time > 400")

    slowRequests.show(false)

    // Calculate ERROR percentage
    println("===== ERROR PERCENTAGE =====")

    val totalRequests = logs.count()

    val errorPercentage =
      (errorCount.toDouble / totalRequests.toDouble) * 100

    println(f"ERROR percentage: $errorPercentage%.2f%%")

    // Calculate average response time by URL
    println("===== AVERAGE RESPONSE TIME BY URL =====")

    val averageResponseByUrl = logs
      .groupBy("url")
      .agg(avg("response_time").alias("average_response_time"))
      .orderBy(desc("average_response_time"))

    averageResponseByUrl.show(false)

    // Count ERROR logs by URL
    println("===== ERROR COUNT BY URL =====")

    val errorCountByUrl = errorLogs
      .groupBy("url")
      .count()
      .orderBy(desc("count"))

    errorCountByUrl.show(false)

    // Complete summary
    println("========================================")
    println("        SPARK LOG ANALYSIS SUMMARY")
    println("========================================")

    val successfulRequests = logs
      .filter("status = 200")
      .count()

    val slowRequestCount = slowRequests.count()

    val topUrl = urlCounts
      .first()
      .getAs[String]("url")

    val topUrlCount = urlCounts
      .first()
      .getAs[Long]("count")

    val topIp = ipCounts
      .first()
      .getAs[String]("ip")

    val topIpCount = ipCounts
      .first()
      .getAs[Long]("count")

    println(s"Total Requests       : $totalRequests")
    println(s"Successful Requests  : $successfulRequests")
    println(s"ERROR Requests       : $errorCount")
    println(f"ERROR Percentage     : $errorPercentage%.2f%%")
    println(f"Average Response Time: $averageResponseTime%.2f ms")
    println(s"Slow Requests        : $slowRequestCount")
    println(s"Most Requested URL   : $topUrl ($topUrlCount requests)")
    println(s"Most Active IP       : $topIp ($topIpCount requests)")

    println("========================================")

    // Stop Spark
    spark.stop()
  }
}
