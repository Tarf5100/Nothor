import org.apache.spark.sql.SparkSession

object RDDOperations {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RDD Operations - UK Accidents")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Final_data (1).csv")

    val rdd = df.rdd

    // =========================================================
    // 1) Severe accidents by weather
    // Transformations: filter, map, reduceByKey, sortBy
    // Action: take
    // =========================================================
    val severeByWeather = rdd
      .filter(row => {
        val sev = row.getAs[Int]("Accident_Severity_idx")
        sev == 1 || sev == 2
      })
      .map(row => (row.getAs[Int]("Weather_Conditions_idx"), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    println("=== Severe accidents by weather ===")
    severeByWeather.take(10).foreach(println)

    // =========================================================
    // 2) Accidents by road type
    // Transformations: map, reduceByKey, sortBy
    // Action: collect
    // =========================================================
    val accidentsByRoadType = rdd
      .map(row => (row.getAs[Int]("Road_Type_idx"), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    println("=== Accidents by road type ===")
    accidentsByRoadType.collect().foreach(println)

    // =========================================================
    // 3) Total number of records
    // Action: count
    // =========================================================
    val totalRecords = rdd.count()
    println("=== Total number of records ===")
    println(totalRecords)

    // =========================================================
    // 4) First row in the dataset
    // Action: first
    // =========================================================
    val firstRow = rdd.first()
    println("=== First row ===")
    println(firstRow)

    // =========================================================
    // 5) Total accidents using reduce
    // Transformation: map
    // Action: reduce
    // =========================================================
    val totalAccidents = rdd
      .map(_ => 1)
      .reduce(_ + _)

    println("=== Total accidents using reduce ===")
    println(totalAccidents)

    // =========================================================
    // 6) Average severity by speed limit
    // Transformations: map, groupByKey, mapValues, sortBy
    // Action: take
    // =========================================================
    val avgSeverityBySpeed = rdd
      .map(row => (
        row.getAs[Int]("Speed_limit"),
        row.getAs[Int]("Accident_Severity_idx")
      ))
      .groupByKey()
      .mapValues(values => values.sum.toDouble / values.size)
      .sortBy(_._1, ascending = true)

    println("=== Average severity by speed limit ===")
    avgSeverityBySpeed.take(10).foreach(println)

    // =========================================================
    // 7) Accidents by urban vs rural area
    // Transformations: map, reduceByKey, sortBy
    // Action: take
    // =========================================================
    val accidentsByArea = rdd
      .map(row => (row.getAs[Int]("Urban_or_Rural_Area_idx"), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    println("=== Accidents by urban vs rural area ===")
    accidentsByArea.take(10).foreach(println)

    spark.stop()
  }
}