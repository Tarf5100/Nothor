import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.classification.{LogisticRegression, DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object MLOperationsImproved {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Improved ML Operations - Accident Severity")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // =========================================================
    // 1) Load final preprocessed dataset
    // =========================================================
    val dfRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Final_data (2).csv")

    println("=== Raw Schema ===")
    dfRaw.printSchema()

    // =========================================================
    // 2) Feature Engineering
    // =========================================================
    val dfFe = dfRaw
      .withColumn(
        "Is_Night",
        when(col("Hour_of_Day") >= 18 || col("Hour_of_Day") <= 5, 1).otherwise(0)
      )
      .withColumn(
        "High_Speed",
        when(col("Speed_limit") >= 60, 1).otherwise(0)
      )
      .withColumn(
        "Rush_Hour",
        when(
          col("Hour_of_Day").between(7, 9) || col("Hour_of_Day").between(16, 18),
          1
        ).otherwise(0)
      )

    // =========================================================
    // 3) Select label + feature columns
    //    Important:
    //    - Removed Year to avoid temporal leakage
    //    - Removed Latitude/Longitude direct usage
    // =========================================================
    val candidateFeatureCols = Seq(
      "Speed_limit",
      "Number_of_Vehicles",
      "Hour_of_Day",
      "Road_Type_idx",
      "Weather_Conditions_idx",
      "Road_Surface_Conditions_idx",
      "Light_Conditions_idx",
      "Urban_or_Rural_Area_idx",
      "Day_of_Week_idx",
      "Time_Period_idx",
      "High_Risk_Conditions",
      "Is_Night",
      "High_Speed",
      "Rush_Hour"
    )

    val featureCols = candidateFeatureCols.filter(dfFe.columns.contains)

    println("=== Selected Feature Columns ===")
    featureCols.foreach(println)

    val requiredCols = ("Accident_Severity_idx" +: featureCols).distinct
    val dfSelected = dfFe.select(requiredCols.map(col): _*)

    // =========================================================
    // 4) Basic ML cleaning
    // =========================================================
    val df = dfSelected
      .na.drop(Seq("Accident_Severity_idx"))
      .na.fill(0)

    println(s"=== Rows after ML cleaning: ${df.count()} ===")

    // =========================================================
    // 5) Create label column
    // =========================================================
    val dfLabeled = df.withColumn("label", col("Accident_Severity_idx").cast("double"))

    // =========================================================
    // 6) Assemble features
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
    // 9) Scaling for Logistic Regression only
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
    // 10) Evaluators
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
    // 11) Logistic Regression
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
    // 12) Decision Tree
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
    // 13) Random Forest
    // =========================================================
    println("\n==================================================")
    println("MODEL 3: Random Forest")
    println("==================================================")

    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(150)
      .setMaxDepth(12)
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
    // 14) Comparison Summary
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
    // 15) Best model selection
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
    // 16) Feature importances from Random Forest
    // =========================================================
    println("\n=== Random Forest Feature Importances ===")
    val importances = rfModel.featureImportances.toArray
    val featureImportancePairs = featureCols.zip(importances).sortBy(-_._2)

    featureImportancePairs.foreach { case (feature, importance) =>
      println(f"$feature%-30s -> $importance%.6f")
    }

    // =========================================================
    // 17) Confusion Matrix for Random Forest
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