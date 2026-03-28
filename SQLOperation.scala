import org.apache.spark.sql.SparkSession

object SQLOperations {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SQL Operations - UK Accidents")
      .master("local[*]")
      .getOrCreate()

    // Read the preprocessed CSV file as a DataFrame
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Final_data (1).csv")

    // Register the DataFrame as a temporary SQL view
    df.createOrReplaceTempView("accidents")

    // =========================================================
    // Query 1: Accidents by road type
    // Purpose: Count accidents for each road type
    // =========================================================
    val q1 = spark.sql("""
      SELECT
        Road_Type_idx,
        COUNT(*) AS total_accidents
      FROM accidents
      GROUP BY Road_Type_idx
      ORDER BY total_accidents DESC
    """)

    println("=== Query 1: Accidents by road type ===")
    q1.show(10, false)

    // =========================================================
    // Query 2: Severe accidents by weather condition
    // Purpose: Show which weather conditions have the most severe accidents
    // =========================================================
    val q2 = spark.sql("""
      SELECT
        Weather_Conditions_idx,
        COUNT(*) AS severe_accidents
      FROM accidents
      WHERE Accident_Severity_idx IN (1, 0)
      GROUP BY Weather_Conditions_idx
      ORDER BY severe_accidents DESC
    """)

    println("=== Query 2: Severe accidents by weather ===")
    q2.show(10, false)

    // =========================================================
    // Query 3: Average severity by speed limit
    // Purpose: Compare accident severity across speed limit categories
    // =========================================================
    val q3 = spark.sql("""
      SELECT
        Speed_limit,
        AVG(Accident_Severity_idx) AS avg_severity
      FROM accidents
      GROUP BY Speed_limit
      ORDER BY Speed_limit ASC
    """)

    println("=== Query 3: Average severity by speed limit ===")
    q3.show(20, false)

    // =========================================================
    // Query 4: Accidents by urban or rural area
    // Purpose: Show accident distribution by area type
    // =========================================================
    val q4 = spark.sql("""
      SELECT
        Urban_or_Rural_Area_idx,
        COUNT(*) AS total_accidents
      FROM accidents
      GROUP BY Urban_or_Rural_Area_idx
      ORDER BY total_accidents DESC
    """)

    println("=== Query 4: Accidents by urban/rural area ===")
    q4.show(10, false)

    // =========================================================
    // Query 5: Weather categories with more than 500 severe accidents
    // Purpose: Demonstrate HAVING with grouped results
    // =========================================================
    val q5 = spark.sql("""
      SELECT
        Weather_Conditions_idx,
        COUNT(*) AS severe_accidents
      FROM accidents
      WHERE Accident_Severity_idx IN (1,0)
      GROUP BY Weather_Conditions_idx
      HAVING COUNT(*) > 500
      ORDER BY severe_accidents DESC
    """)

    println("=== Query 5: Weather categories with more than 500 severe accidents ===")
    q5.show(10, false)

    // Optional: display schema to verify columns
    println("=== DataFrame Schema ===")
    df.printSchema()

    spark.stop()
  }
}