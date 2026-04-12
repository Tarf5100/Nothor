error id: 7B24F233620AC68017BC6B6C3469744D
file:///C:/MAMP/htdocs/Nothor/MLOperations.scala
### dotty.tools.dotc.core.UnpicklingError: Could not read definition class LowPriorityImplicits2 in <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\3.8.2\scala-library-3.8.2.jar(scala/LowPriorityImplicits2.tasty). Caused by the following exception:
java.lang.AssertionError: assertion failed: `-Xread-docs` enabled, but no `docCtx` is set.

Run with -Ydebug-unpickling to see full stack trace.

occurred in the presentation compiler.



action parameters:
offset: 2
uri: file:///C:/MAMP/htdocs/Nothor/MLOperations.scala
text:
```scala

@@import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object MLOperations {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Phase 5 - Machine Learning with Spark")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // =========================================================
    // 1) Load final preprocessed dataset
    // =========================================================
    val dfRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:/MAMP/htdocs/Nothor/Final_data (1).csv")
      // إذا الملف في نفس الفولدر فقط، تقدرين تحطين:
      // .csv("Final_data (1).csv")

    println("=== Raw Schema ===")
    dfRaw.printSchema()

    // =========================================================
    // 2) Define label and feature columns
    // =========================================================
    // label = Accident_Severity_idx
    // نستبعد أي أعمدة فيها تسريب أو غير مناسبة:
    // - Accident_Index (ID)
    // - Accident_Severity_idx (label)
    // - Date_std / Time_std (timestamp)
    // - أي عمود مشتق مباشرة من الشدة أو الإصابات لو موجود
    //   مثل Severity_Score أو Vehicles_Per_Casualty
    //   أو Number_of_Casualties (يفضل استبعاده لتجنب leakage)
    val candidateFeatureCols = Seq(
      "Speed_limit",
      "Year",
      "Latitude",
      "Longitude",
      "Number_of_Vehicles",
      "Hour_of_Day",
      "Road_Type_idx",
      "Weather_Conditions_idx",
      "Road_Surface_Conditions_idx",
      "Light_Conditions_idx",
      "Urban_or_Rural_Area_idx",
      "Day_of_Week_idx",
      "Time_Period_idx",
      "High_Risk_Conditions"
    )

    val featureCols = candidateFeatureCols.filter(dfRaw.columns.contains)

    println("=== Selected Feature Columns ===")
    featureCols.foreach(println)

    // =========================================================
    // 3) Keep only needed columns + label
    // =========================================================
    val requiredCols = ("Accident_Severity_idx" +: featureCols).distinct

    val dfSelected = dfRaw.select(requiredCols.map(col): _*)

    // =========================================================
    // 4) Basic cleaning for ML
    // =========================================================
    // نحذف الصفوف اللي label فيها null
    // ونملي القيم العددية الناقصة بـ 0 لتسهيل التدريب
    val numericCols = dfSelected.schema.fields.map(_.name)

    val df = dfSelected
      .na.drop(Seq("Accident_Severity_idx"))
      .na.fill(0)

    println(s"=== Rows after ML cleaning: ${df.count()} ===")

    // =========================================================
    // 5) Convert label to Double as required by Spark ML
    // =========================================================
    val dfLabeled = df.withColumn("label", col("Accident_Severity_idx").cast("double"))

    // =========================================================
    // 6) Assemble features into a single vector
    // =========================================================
    val assembler = new VectorAssembler()
      .setInputCols(featureCols.toArray)
      .setOutputCol("features")

    val assembledDf = assembler.transform(dfLabeled).select("label", "features")

    println("=== Sample of Assembled Data ===")
    assembledDf.show(5, truncate = false)

    // =========================================================
    // 7) Train / Test split
    // =========================================================
    val Array(trainData, testData) = assembledDf.randomSplit(Array(0.7, 0.3), seed = 42)

    println(s"Training rows: ${trainData.count()}")
    println(s"Test rows: ${testData.count()}")

    // =========================================================
    // 8) Baseline model (majority class)

> مها:
// =========================================================
    val majorityLabel = trainData
      .groupBy("label")
      .count()
      .orderBy(desc("count"))
      .first()
      .getDouble(0)

    val baselinePredictions = testData.withColumn("prediction", lit(majorityLabel))

    val baselineAccuracy = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
      .evaluate(baselinePredictions)

    println(s"=== Baseline Accuracy (majority class) = $baselineAccuracy ===")

    // =========================================================
    // 9) Train Random Forest model
    // =========================================================
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(100)
      .setMaxDepth(10)
      .setSeed(42)

    val model = rf.fit(trainData)

    // =========================================================
    // 10) Predictions
    // =========================================================
    val predictions = model.transform(testData)

    println("=== Sample Predictions ===")
    predictions.select("label", "prediction", "probability").show(10, truncate = false)

    // =========================================================
    // 11) Evaluation metrics
    // =========================================================
    val accuracyEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val f1Evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("f1")

    val weightedPrecisionEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")

    val weightedRecallEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")

    val accuracy = accuracyEvaluator.evaluate(predictions)
    val f1 = f1Evaluator.evaluate(predictions)
    val weightedPrecision = weightedPrecisionEvaluator.evaluate(predictions)
    val weightedRecall = weightedRecallEvaluator.evaluate(predictions)

    println("=== Model Evaluation ===")
    println(s"Accuracy           = $accuracy")
    println(s"F1 Score           = $f1")
    println(s"Weighted Precision = $weightedPrecision")
    println(s"Weighted Recall    = $weightedRecall")

    // =========================================================
    // 12) Confusion matrix
    // =========================================================
    println("=== Confusion Matrix ===")
    predictions
      .groupBy("label", "prediction")
      .count()
      .orderBy("label", "prediction")
      .show(50, truncate = false)

    // =========================================================
    // 13) Feature importances
    // =========================================================
    println("=== Feature Importances ===")
    val importances = model.featureImportances.toArray
    val featureImportancePairs = featureCols.zip(importances).sortBy(-_._2)

    featureImportancePairs.foreach { case (feature, importance) =>
      println(f"$feature%-30s -> $importance%.6f")
    }

    // =========================================================
    // 14) Compare model vs baseline
    // =========================================================
    println("=== Comparison to Baseline ===")
    println(s"Baseline Accuracy = $baselineAccuracy")
    println(s"Model Accuracy    = $accuracy")
    println(s"Improvement       = ${accuracy - baselineAccuracy}")

    spark.stop()
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