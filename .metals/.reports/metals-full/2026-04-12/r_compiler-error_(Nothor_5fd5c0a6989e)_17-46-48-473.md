error id: 7B24F233620AC68017BC6B6C3469744D
file:///C:/MAMP/htdocs/Nothor/MLOperations.scala
### dotty.tools.dotc.core.UnpicklingError: Could not read definition class LowPriorityImplicits in <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\3.8.2\scala-library-3.8.2.jar(scala/LowPriorityImplicits.tasty). Caused by the following exception:
java.lang.AssertionError: assertion failed: `-Xread-docs` enabled, but no `docCtx` is set.

Run with -Ydebug-unpickling to see full stack trace.

occurred in the presentation compiler.



action parameters:
offset: 10795
uri: file:///C:/MAMP/htdocs/Nothor/MLOperations.scala
text:
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.classification.{LogisticRegression, DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object MLOperations {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Phase 5 - Multiple ML Models with Scaling")
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
      // أو إذا الملف في نفس الفولدر:
      // .csv("Final_data (1).csv")

    println("=== Raw Schema ===")
    dfRaw.printSchema()

    // =========================================================
    // 2) Select label + feature columns
    // =========================================================
    // excluded:
    // - Accident_Index (ID)
    // - Accident_Severity_idx (label)
    // - Number_of_Casualties to reduce leakage
    // - any directly derived leakage columns
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

    val requiredCols = ("Accident_Severity_idx" +: featureCols).distinct
    val dfSelected = dfRaw.select(requiredCols.map(col): _*)

    // =========================================================
    // 3) Basic ML cleaning
    // =========================================================
    val df = dfSelected
      .na.drop(Seq("Accident_Severity_idx"))
      .na.fill(0)

    println(s"=== Rows after ML cleaning: ${df.count()} ===")

    // =========================================================
    // 4) Create label column
    // =========================================================
    val dfLabeled = df.withColumn("label", col("Accident_Severity_idx").cast("double"))

    // =========================================================
    // 5) Assemble features
    // =========================================================
    val assembler = new VectorAssembler()
      .setInputCols(featureCols.toArray)
      .setOutputCol("features")

    val assembledDf = assembler.transform(dfLabeled).select("label", "features")

    println("=== Sample of Assembled Data ===")
    assembledDf.show(5, truncate = false)

    // =========================================================
    // 6) Train / Test split
    // =========================================================
    val Array(trainData, testData) = assembledDf.randomSplit(Array(0.7, 0.3), seed = 42)

    println(s"Training rows: ${trainData.count()}")
    println(s"Test rows: ${testData.count()}")

    // =========================================================
    // 7) Baseline model (majority class)
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

    println(s"\n=== Baseline Accuracy (majority class) = $baselineAccuracy ===")

    // =========================================================
    // 8) Scaling for Logistic Regression only
    // =========================================================
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    val scalerModel = scaler.fit(trainData)
    val scaledTrainData = scalerModel.transform(trainData)
    val scaledTestData = scalerModel.transform(testData)

    // =========================================================
    // 9) Evaluators
    // =========================================================
    val accuracyEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val f1Evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("f1")

    val precisionEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")

    val recallEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")

    // =========================================================
    // 10) Logistic Regression (with scaling)
    // =========================================================
    println("\n==================================================")
    println("MODEL 1: Logistic Regression")
    println("==================================================")

    val lr = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("scaledFeatures")
      .setMaxIter(100)
      .setRegParam(0.01)
      .setElasticNetParam(0.0)

    val lrModel = lr.fit(scaledTrainData)
    val lrPredictions = lrModel.transform(scaledTestData)

    val lrAccuracy = accuracyEvaluator.evaluate(lrPredictions)
    val lrF1 = f1Evaluator.evaluate(lrPredictions)
    val lrPrecision = precisionEvaluator.evaluate(lrPredictions)
    val lrRecall = recallEvaluator.evaluate(lrPredictions)

    println(s"Accuracy           = $lrAccuracy")
    println(s"F1 Score           = $lrF1")
    println(s"Weighted Precision = $lrPrecision")
    println(s"Weighted Recall    = $lrRecall")

    // =========================================================
    // 11) Decision Tree (without scaling)
    // =========================================================
    println("\n==================================================")
    println("MODEL 2: Decision Tree")
    println("==================================================")

    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxDepth(10)
      .setSeed(42)

    val dtModel = dt.fit(trainData)
    val dtPredictions = dtModel.transform(testData)

    val dtAccuracy = accuracyEvaluator.evaluate(dtPredictions)
    val dtF1 = f1Evaluator.evaluate(dtPredictions)
    val dtPrecision = precisionEvaluator.evaluate(dtPredictions)
    val dtRecall = recallEvaluator.evaluate(dtPredictions)

    println(s"Accuracy           = $dtAccuracy")
    println(s"F1 Score           = $dtF1")
    println(s"Weighted Precision = $dtPrecision")
    println(s"Weighted Recall    = $dtRecall")

    // =========================================================
    // 12) Random Forest (without scaling)
    // =========================================================
    println("\n==================================================")
    println("MODEL 3: Random Forest")
    println("==================================================")

    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(100)
      .setMaxDepth(10)
      .setSeed(42)

    val rfModel = rf.fit(trainData)
    val rfPredictions = rfModel.transform(testData)

    val rfAccuracy = accuracyEvaluator.evaluate(rfPredictions)
    val rfF1 = f1Evaluator.evaluate(rfPredictions)
    val rfPrecision = precisionEvaluator.evaluate(rfPredictions)
    val rfRecall = recallEvaluator.evaluate(rfPredictions)

    println(s"Accuracy           = $rfAccuracy")
    println(s"F1 Score           = $rfF1")
    println(s"Weighted Precision = $rfPrecision")
    println(s"Weighted Recall    = $rfRecall")

    // =========================================================
    // 13) Comparison Summary
    // =========================================================
    println("\n==================================================")
    println("MODEL COMPARISON SUMMARY")
    println("==================================================")
    println(f"${"Model"}%-22s ${"Accuracy"}%-12s ${"F1"}%-12s ${"Precision"}%-12s ${"Recall"}%-12s")
    println("-" * 80)
    println(f"${"Baseline"}%-22s $baselineAccuracy%-12.4f ${"-"}%-12s ${"-"}%-12s ${"-"}%-12s")
    println(f"${"Logistic Regression"}%-22s $lrAccuracy%-12.4f $lrF1%-12.4f $lrPrecision%-12.4f $lrRecall%-12.4f")
    println(f"${"Decision Tree"}%-22s $dtAccuracy%-12.4f $dtF1%-12.4f $dtPrecision%-12.4f $dtRecall%-12.4f")
    println(f"${"Random Forest"}%-22s $rfAccuracy%-12.4f $rfF1%-12.4f $rfPrecision%-12.4f $rfRecall%-12.4f")

    // =========================================================
    // 14) Best model selection
    // =========================================================
    val modelScores = Seq(
      ("Logistic Regression", lrAccuracy, lrF1),
      ("Decision Tree", dtAccuracy, dtF1),
      ("Random Forest", rfAccuracy, rfF1)
    )

    val bestModel = modelScores.maxBy(_._2)

    println("\n=== Best Model Based on Accuracy ===")
    println(s"Best Model = ${bestModel._1}")
    println(s"Best Accuracy = ${bestModel._2}")
    println(s"Best F1 Score = ${bestModel._3}")

    // =========================================================
    // 15) Feature importances from Random Forest
    // =========================================================
    println("\n=== Random Forest Feature Importances ===")
    val importances = rfModel.featureImportances.toArray
    val featureImportancePairs = featureCols.zip(importances).sortBy(-_._2)

    featureImportancePairs.foreach { case (feature, importance) =>
      println(f"$feature%-30s -> $importance%.6f")
    }

    // =============@@============================================
    // 16) Confusion Matrix for Random Forest
    // =========================================================
    println("\n=== Confusion Matrix (Random Forest) ===")
    rfPredictions
      .groupBy("label", "prediction")
      .count()
      .orderBy("label", "prediction")
      .show(50, truncate = false)

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

dotty.tools.dotc.core.UnpicklingError: Could not read definition class LowPriorityImplicits in <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\3.8.2\scala-library-3.8.2.jar(scala/LowPriorityImplicits.tasty). Caused by the following exception:
java.lang.AssertionError: assertion failed: `-Xread-docs` enabled, but no `docCtx` is set.

Run with -Ydebug-unpickling to see full stack trace.