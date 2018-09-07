/* RiaktrChallenge.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RiaktrChallenge {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Riaktr Challenge").getOrCreate()
    
    import spark.implicits._
    
     if (args.length != 3) {
        println("Number of arguments needed: 3")
    }

    val cdrPath = args(0)
    val cellPath = args(1)
    val outputFilePath = args(2)

    // Reading the data
    val cdr = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(cdrPath + "/cdrs.csv")
    val cell = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(cellPath + "/cells.csv")

    // Caching
    cdr.cache()
    cell.cache()

    // What is the most used cell?
    val mostUsedCellId = cdr.groupBy("cell_id").count().sort($"count".desc).select($"cell_id".cast("string")).first.getString(0)

    // Number of distinct calls
    val numberDistinctCalls = cdr.count()

    // Number of dropped calls
    val NumberDroppedCalls = cdr.filter($"dropped" === 1).count()

    // Total duration of the calls
    val totalDurationCalls = cdr.agg(sum("duration").cast("double")).first.getDouble(0)

    // Total duration of the international calls
    val totalDurationInternationalCalls = cdr.filter($"type" === "international").agg(sum("duration").cast("double")).first.getDouble(0)

    // Average duration of the on-net calls
    val averageDurationOnNetCalls = cdr.filter($"type" === "on-net").agg(mean("duration").cast("double")).first.getDouble(0)

    // Latitude and Longitude of the most used cell
    val r7 = cell.filter($"cell_id" === mostUsedCellId)
    val latitudeMostUsedCell = cell.filter($"cell_id" === mostUsedCellId).select($"latitude".cast("double")).first.getDouble(0)
    val longitudeMostUsedCell = cell.filter($"cell_id" === mostUsedCellId).select($"longitude".cast("double")).first.getDouble(0)

    // Number of calls that lasted <= 10 min
    val numberCallsLastingLessThanTenMin = cdr.filter($"duration" <= 10).count()

    // Number of calls relayed by the most used cell
    val numberCallsRelayedByMostUsedCell = cdr.filter($"cell_id" === mostUsedCellId).count()

    // Top 3 callee ids
    val r10 = cdr.groupBy("callee_id").agg(sum("duration")).sort($"sum(duration)".desc).select("callee_id").head(3)
    val topFirstCalleeId = r10(0).getInt(0)
    val topSecondCalleeId = r10(1).getInt(0)
    val topThirdCalleeId = r10(2).getInt(0)

    // Creating output dataframe
    val outputCSV = Seq(
      (mostUsedCellId				.toString, "Most used cell (cell_id)"),
      (numberDistinctCalls			.toString, "Number of distinct calls"),
      (NumberDroppedCalls			.toString, "Number of dropped calls"),
      (totalDurationCalls			.toString, "Total call duration"),
      (totalDurationInternationalCalls	.toString, "Total call duration for international calls"),
      (averageDurationOnNetCalls		.toString, "Average duration on-net calls"),
      (latitudeMostUsedCell			.toString, "Latitude of the most used cell"),
      (longitudeMostUsedCell			.toString, "Longitude of the most used cell"),
      (numberCallsLastingLessThanTenMin	.toString, "Number of calls lasting less than 10 minutes (included)"),
      (numberCallsRelayedByMostUsedCell	.toString, "Number of calls relayed by the most used cell"),
      (topFirstCalleeId			.toString, "Top first callee id"),
      (topSecondCalleeId			.toString, "Top second callee id"),
      (topThirdCalleeId			.toString, "Top third callee id")
    ).toDF("Value", "Description")

    // Writing output dataframe to csv
    val tmpPath = outputFilePath + "/tmpOutput"

    outputCSV.coalesce(1).write.option("header", "true").mode("overwrite").csv(tmpPath)

    val tmpDir = new java.io.File(tmpPath)
    val tmpCsvFilePath = tmpDir.listFiles.filter(x => x.toPath.toString.matches(tmpPath + "/part-00000.*"))(0).toString
    (new java.io.File(tmpCsvFilePath)).renameTo(new java.io.File(outputFilePath + "/results.csv"))
  
    tmpDir.listFiles.foreach(x => x.delete)
    tmpDir.delete
    
    spark.stop()
  }
}

