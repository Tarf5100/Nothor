error id: 7B24F233620AC68017BC6B6C3469744D
file:///C:/MAMP/htdocs/Nothor/src/main/scala/preprocessing/Transformation.scala
### dotty.tools.dotc.core.UnpicklingError: Could not read definition object Predef in <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\3.8.2\scala-library-3.8.2.jar(scala/Predef.tasty). Caused by the following exception:
java.lang.AssertionError: assertion failed: `-Xread-docs` enabled, but no `docCtx` is set.

Run with -Ydebug-unpickling to see full stack trace.

occurred in the presentation compiler.



action parameters:
offset: 6534
uri: file:///C:/MAMP/htdocs/Nothor/src/main/scala/preprocessing/Transformation.scala
text:
```scala
package preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Transformation {

  /**

   *
   * Note: Cleaning.scala already handled:
   *   - String → Integer  (Speed_limit, Number_of_Vehicles, Number_of_Casualties, Year)
   *   - String → Date     (Date → Date_std)
   *   - String → Timestamp (Time → Time_std)
   *
   * This step adds:
   *   1) String → Double  (Latitude, Longitude)
   *   2) Timestamp → Hour_of_Day Integer  (extracted from Time_std)
   */
  def run(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    println("\n[TRANSFORMATION] Starting...")
    println(s"[TRANSFORMATION] Rows   : ${df.count()}")
    println(s"[TRANSFORMATION] Columns: ${df.columns.length}")

    // =========================================================
    // STEP 1 – Cast Latitude and Longitude to Double
    // =========================================================
    // WHY: Latitude and Longitude were read as String by inferSchema
    //      because some rows contain non-numeric placeholders.
    //      Casting to Double makes them usable in distance calculations
    //      and as numeric ML features.
    // HOW: Use .cast(DoubleType); invalid strings become null.

    println("\n[TRANSFORMATION] Step 1 – Cast Latitude & Longitude to Double")

    val df1 = df
      .withColumn("Latitude",  col("Latitude").cast(DoubleType))
      .withColumn("Longitude", col("Longitude").cast(DoubleType))

    val latNulls = df1.filter(col("Latitude").isNull).count()
    val lonNulls = df1.filter(col("Longitude").isNull).count()
    println(s"[TRANSFORMATION]   Latitude  nulls after cast : $latNulls")
    println(s"[TRANSFORMATION]   Longitude nulls after cast : $lonNulls")

    // =========================================================
    // STEP 2 – Extract Hour_of_Day from Time_std
    // =========================================================
    // WHY: The raw Time column (e.g. "17:42") was already parsed to a
    //      Timestamp in Cleaning (Time_std). Extracting the integer hour
    //      (0–23) creates a numeric feature that captures time-of-day
    //      patterns (rush hour, night-time, etc.) for ML models.
    // HOW: Apply Spark's hour() function on Time_std.

    println("\n[TRANSFORMATION] Step 2 – Extract Hour_of_Day from Time_std")

    val df2 =
      if (df1.columns.contains("Time_std")) {
        df1.withColumn("Hour_of_Day", hour(col("Time_std")).cast(IntegerType))
      } else {
        println("[TRANSFORMATION]   WARNING: Time_std column not found – skipping Hour_of_Day extraction")
        df1
      }

    val hourNulls = df2.filter(col("Hour_of_Day").isNull).count()
    println(s"[TRANSFORMATION]   Hour_of_Day nulls: $hourNulls")
    println("[TRANSFORMATION]   Sample Hour_of_Day distribution:")
    df2.groupBy("Hour_of_Day").count().orderBy("Hour_of_Day").show(24, truncate = false)

    // =========================================================
    // FINAL SUMMARY PART 1
    // =========================================================
    println(s"\n[TRANSFORMATION] Completed.")
    println(s"[TRANSFORMATION] Final rows   : ${df2.count()}")
    println(s"[TRANSFORMATION] Final columns: ${df2.columns.length}")
    println(s"[TRANSFORMATION] New columns added: Latitude (Double), Longitude (Double), Hour_of_Day (Int)")

    // ================================================================
    // ================================================================

    var currentDf = df2

    // =========================================================
    // STEP 3 – Data Type Conversions
    // =========================================================
    println("\n[STEP 3] Converting data types...")
    
    // Ensure all numeric columns are properly typed
    val numericColumns = Seq(
      "Year",
      "Speed_limit",
      "Number_of_Vehicles",
      "Number_of_Casualties",
      "Hour_of_Day",
      "Latitude",
      "Longitude",
    ).filter(currentDf.columns.contains)
    
    numericColumns.foreach { colName =>
      val dataType = if (colName.contains("Latitude") || colName.contains("Longitude")) DoubleType else IntegerType
      currentDf = currentDf.withColumn(colName, col(colName).cast(dataType))
      println(s" $colName → ${if(dataType == DoubleType) "Double" else "Integer"}")
    }

    // =========================================================
    // STEP 4 – Categorical Encoding 
    // =========================================================
    println("\n[STEP 4] Encoding categorical variables...")
    
    val categoricalColumns = Seq(
      "Road_Type",
      "Weather_Conditions",
      "Road_Surface_Conditions",
      "Light_Conditions",
      "Urban_or_Rural_Area",
      "Accident_Severity",
      "Day_of_Week" 
    ).filter(currentDf.columns.contains)
    
    println(s"  Categorical columns: ${categoricalColumns.mkString(", ")}")
    
    // Index Encoding 
    println("\n  Using Index Encoding...")
    
    categoricalColumns.foreach { colName =>
      val indexedColName = s"${colName}_idx"
      
      // Get distinct values (excluding nulls)
      val distinctValues = currentDf.select(colName).distinct()
        .filter(col(colName).isNotNull)
        .collect()
        .map(_.getString(0))
        .sorted
      
      println(s"    $colName → $indexedColName (${distinctValues.length} categories)")
      
      // Create mapping using when/otherwise
      var whenExpr = when(col(colName).isNull, lit(null))
      distinctValues.zipWithIndex.foreach { case (value, index) =>
        whenExpr = whenExpr.when(col(colName) === value, lit(index))
      }
      
      currentDf = currentDf.withColumn(indexedColName, whenExpr.cast(IntegerType))
    }


    // =========================================================
    // STEP 5 – Feature Engineering
    // =========================================================
    println("\n[STEP 5] Engineering domain features...")
    
    
    // Time period from hour
    if (currentDf.columns.contains("Hour_of_Day")) {
      currentDf = currentDf.withColumn("Time_Period",
        when(col("Hour_of_Day").between(5, 11), "morning")
        .when(col("Hour_of_Day").between(12, 16), "afternoon")
        .when(col("Hour_of_Day").between(17, 20), "evening")
       @@ .otherwise("night")
      )
      println(" Time_Period (morning/afternoon/evening/night)")
    }


    
    // High risk conditions
    if (Seq("Weather_Conditions", "Road_Surface_Conditions", "Light_Conditions")
        .forall(currentDf.columns.contains)) {
      
      currentDf = currentDf.withColumn("High_Risk_Conditions",
        when(
          (col("Weather_Conditions").contains("rain") || 
           col("Weather_Conditions").contains("snow") ||
           col("Weather_Conditions").contains("fog")) &&
          (col("Road_Surface_Conditions").contains("wet") || 
           col("Road_Surface_Conditions").contains("snow") ||
           col("Road_Surface_Conditions").contains("ice")) &&
          (col("Light_Conditions").contains("dark") || 
           col("Light_Conditions").contains("night")),
          1
        ).otherwise(0)
      )
      println(" High_Risk_Conditions (risk indicator)")
    }

    // =========================================================
    //  Second Categorical Encoding for new features
    // =========================================================
    println("\n[STEP 6] Second encoding pass - encoding new features...")
    
    val categoricalColumns2 = Seq("Time_Period").filter(currentDf.columns.contains)
    
    if (categoricalColumns2.nonEmpty) {
      println(s"  New categorical columns: ${categoricalColumns2.mkString(", ")}")
      
      categoricalColumns2.foreach { colName =>
        val indexedColName = s"${colName}_idx"
        
        val distinctValues = currentDf.select(colName).distinct()
          .filter(col(colName).isNotNull)
          .collect()
          .map(_.getString(0))
          .sorted
        
        println(s"    $colName → $indexedColName (${distinctValues.length} categories)")
        
        var whenExpr = when(col(colName).isNull, lit(null))
        distinctValues.zipWithIndex.foreach { case (value, index) =>
          whenExpr = whenExpr.when(col(colName) === value, lit(index))
        }
        
        currentDf = currentDf.withColumn(indexedColName, whenExpr.cast(IntegerType))
      }
    } else {
      println("  No new categorical features to encode")
    }
   
 // =========================================================
// STEP 7 – Drop original categorical columns
// =========================================================
println("\n Dropping original categorical columns...")

// Drop the original categorical columns individually
currentDf = currentDf
  .drop("Road_Type")
  .drop("Weather_Conditions")
  .drop("Road_Surface_Conditions")
  .drop("Light_Conditions")
  .drop("Urban_or_Rural_Area")
  .drop("Accident_Severity")
  .drop("Day_of_Week")
  .drop("Time_Period")

println(s"  Dropped columns: Road_Type, Weather_Conditions, Road_Surface_Conditions, Light_Conditions, Urban_or_Rural_Area, Accident_Severity, Day_of_Week, Time_Period")
println(s"  Columns remaining: ${currentDf.columns.length}")


    // =========================================================
    // FINAL SUMMARY PART 2
    // =========================================================
    println("\n" + "="*80)
    println("TRANSFORMATIONS COMPLETED")
    println("="*80)
    
    val originalCols = df2.columns.toSet
    val newCols = currentDf.columns.filterNot(originalCols.contains)
    
    println(s"\nOriginal columns: ${df2.columns.length}")
    println(s"Final columns: ${currentDf.columns.length}")
    println(s"New columns added (${newCols.length}):")
    newCols.grouped(4).foreach { group =>
      println(s"  ${group.mkString(", ")}")
    }
    currentDf
  
  }
}

```


presentation compiler configuration:
Scala version: 3.8.2-bin-nonbootstrapped
Classpath:
<WORKSPACE>\.scala-build\Nothor_d5c0a6989e\classes\main [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala3-library_3\3.8.2\scala3-library_3-3.8.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\3.8.2\scala-library-3.8.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\sourcegraph\semanticdb-javac\0.10.0\semanticdb-javac-0.10.0.jar [exists ], <WORKSPACE>\.scala-build\Nothor_d5c0a6989e\classes\main\META-INF\best-effort [missing ]
Options:
-Xsemanticdb -sourceroot <WORKSPACE> -release 11 -Ywith-best-effort-tasty




#### Error stacktrace:

```

```
#### Short summary: 

dotty.tools.dotc.core.UnpicklingError: Could not read definition object Predef in <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\3.8.2\scala-library-3.8.2.jar(scala/Predef.tasty). Caused by the following exception:
java.lang.AssertionError: assertion failed: `-Xread-docs` enabled, but no `docCtx` is set.

Run with -Ydebug-unpickling to see full stack trace.