/* RiaktrChallenge.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

object RiaktrChallenge {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Riaktr Challenge").getOrCreate()
    
    if (args.length != 3) println("Number of arguments needed: 3")

    val cdrPath = args(0)
    val cellPath = args(1)
    val outputFilePath = args(2)

    // Reading data from csv
    val cdr = readFromCsv(cdrPath + "/cdrs.csv")
    val cell = readFromCsv(cellPath + "/cells.csv")

    // Calculating metrics
    val outputCSV = getMetrics(cdr, cell)

    // Writing output dataframe to csv
    writeDfToCSV(outputCSV, outputFilePath)
    
    spark.stop()
  }


  def readFromCsv(pathAndFileName: String): DataFrame = {
    val spark = SparkSession.builder.getOrCreate()

    spark.read.
      format("csv").
      option("header", "true").
      option("inferSchema", "true").
      load(pathAndFileName)
  }

  def getMetrics(cdr: DataFrame, cell: DataFrame): DataFrame = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    // What is the most used cell?
    // + Number of distinct calls for most used cell 
    // + latitude and longitude of the most used cell (question 1 & 7 & 9)
    val q1q7q9 = cdr.
      groupBy("caller_id", "cell_id").
      agg(count(lit(1)) as "no_of_distinct_calls_most_used_cell").
      withColumn("rnum", rank().over(Window.
        partitionBy("caller_id").
        orderBy($"no_of_distinct_calls_most_used_cell".desc, $"cell_id".desc))).
      filter($"rnum" === 1).
      drop("rnum").
      join(cell, cdr("cell_id") === cell("cell_id"), "left").
      drop("rnum").
      drop(cell("cell_id")).
      withColumnRenamed("cell_id", "most_used_cell").
      withColumnRenamed("longitude", "longitude_most_used_cell").
      withColumnRenamed("latitude", "latitude_most_used_cell")

    // Number of distinct callees + Total duration of the calls (question 2 & 4)
    val q2q4 = cdr.
      groupBy("caller_id").
      agg(countDistinct("callee_id") as "no_of_distinct_callees", 
        sum("duration") as "total_duration").
      withColumnRenamed("caller_id", "caller_id_q2q4")

    // Number of dropped calls (question 3)
    val q3 = cdr.
      filter($"dropped" === 1).
      groupBy("caller_id").
      agg(count(lit(1)) as "no_of_dropped_calls").
      withColumnRenamed("caller_id", "caller_id_q3")

    // Total duration of the international calls (question 5)
    val q5 = cdr.
      filter($"type" === "international").
      groupBy("caller_id").
      agg(sum("duration") as "total_duration_international_calls").
      withColumnRenamed("caller_id", "caller_id_q5")

    // Average duration of the on-net calls (question 6)
    val q6 = cdr.
      filter($"type" === "on-net").
      groupBy("caller_id").
      agg(mean("duration") as "average_duration_on_net_calls").
      withColumnRenamed("caller_id", "caller_id_q6")

    // Number of calls that lasted <= 10 min (question 8)
    val q8 = cdr.
      filter($"duration" <= 10).
      groupBy("caller_id").
      agg(count(lit(1)) as "no_of_distinct_calls_lasting_less_than_10_min").
      withColumnRenamed("caller_id", "caller_id_q8")

    // Top 3 callee ids (question 10)
    val q10 = cdr.
      groupBy("caller_id", "callee_id").
      agg(count(lit(1)) as "no_of_distinct_calls").
      withColumn("rnum", rank().over(Window.
        partitionBy("caller_id").
        orderBy($"no_of_distinct_calls".desc, $"callee_id".desc))).
      withColumn("first_callee", when($"rnum" === 1, cdr("callee_id"))).
      withColumn("second_callee", when($"rnum" === 2, cdr("callee_id"))).
      withColumn("third_callee", when($"rnum" === 3, cdr("callee_id"))).
      groupBy("caller_id").
      agg(max("first_callee") as "first_callee", 
        max("second_callee") as "second_callee", 
        max("third_callee") as "third_callee").
      withColumnRenamed("caller_id", "caller_id_q10")

    // Merging everything together
    val outputCSV = q1q7q9.
      join(q2q4, $"caller_id" === q2q4("caller_id_q2q4"), "left").
      join(q3, $"caller_id" === q3("caller_id_q3"), "left").
      join(q5, $"caller_id" === q5("caller_id_q5"), "left").
      join(q6, $"caller_id" === q6("caller_id_q6"), "left").
      join(q8, $"caller_id" === q8("caller_id_q8"), "left").
      join(q10, $"caller_id" === q10("caller_id_q10"), "left").
      drop("caller_id_q2q4").
      drop("caller_id_q3").
      drop("caller_id_q5").
      drop("caller_id_q6").
      drop("caller_id_q8").
      drop("caller_id_q10")  
    
    outputCSV  
  }

  def writeDfToCSV(outputCSV: DataFrame, outputFilePath: String) = {
    val tmpPath = outputFilePath + "/tmpOutput"

    outputCSV.coalesce(1).write.option("header", "true").mode("overwrite").csv(tmpPath)

    val tmpDir = new java.io.File(tmpPath)
    val tmpCsvFilePath = tmpDir.
      listFiles.
      filter(x => x.toPath.toString.matches(tmpPath + "/part-00000.*"))(0).
      toString
    (new java.io.File(tmpCsvFilePath)).
      renameTo(new java.io.File(outputFilePath + "/results.csv"))
  
    tmpDir.listFiles.foreach(x => x.delete)
    tmpDir.delete
  }
}