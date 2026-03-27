error id: 7B24F233620AC68017BC6B6C3469744D
file:///C:/MAMP/htdocs/Nothor/RDDOperations.scala
### dotty.tools.dotc.core.UnpicklingError: Could not read definition class LowPriorityImplicits2 in <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\3.8.2\scala-library-3.8.2.jar(scala/LowPriorityImplicits2.tasty). Caused by the following exception:
java.lang.AssertionError: assertion failed: `-Xread-docs` enabled, but no `docCtx` is set.

Run with -Ydebug-unpickling to see full stack trace.

occurred in the presentation compiler.



action parameters:
offset: 395
uri: file:///C:/MAMP/htdocs/Nothor/RDDOperations.scala
text:
```scala
import org.apache.spark.sql.{SparkSession, Row}

object RDDOperations {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RDD Operations - UK Accidents")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:/data/final_dataset.csv")@@   // عدلي المسار

    val rdd = df.rdd

    // =========================================================
    // 1) Severe accidents by weather
    // Transformations: filter, map, reduceByKey, sortBy
    // Action: take
    // =========================================================
    val severeByWeather = rdd
      .filter(row => {
        val sev = row.getAs[String]("Accident_Severity")
        sev == "serious" || sev == "fatal"
      })
      .map(row => (row.getAs[String]("Weather_Conditions"), 1))
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
      .map(row => (row.getAs[String]("Road_Type"), 1))
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

    spark.stop()

        // =========================================================
    // 6) Average severity score by speed limit
    // Transformations: map, groupByKey, mapValues, sortBy
    // Action: take
    // =========================================================
    val avgSeverityBySpeed = rdd
      .map(row => (
        row.getAs[Int]("Speed_limit"),
        row.getAs[Int]("Severity_Score")
      ))
      .groupByKey()
      .mapValues(values => values.sum.toDouble / values.size)
      .sortBy(_._1, ascending = true)

    println("=== Average severity by speed limit ===")
    avgSeverityBySpeed.take(10).foreach(println)
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

dotty.tools.dotc.core.UnpicklingError: Could not read definition class LowPriorityImplicits2 in <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\3.8.2\scala-library-3.8.2.jar(scala/LowPriorityImplicits2.tasty). Caused by the following exception:
java.lang.AssertionError: assertion failed: `-Xread-docs` enabled, but no `docCtx` is set.

Run with -Ydebug-unpickling to see full stack trace.