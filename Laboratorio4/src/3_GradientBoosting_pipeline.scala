import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegressionModel

val file_location = "/home/jc/Documentos/Macrodatos/macrodata-projects/Laboratorio4/Data/stroke_clean.csv"

// COMMAND ----------
val data = spark
            .read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .csv(file_location).cache()

// COMMAND ----------
data.show(5)
/*
+-----+------+---+------------+-------------+------------+-------------+--------------+-----------------+----+---------------+------+
|   id|gender|age|hypertension|heart_disease|ever_married|    work_type|Residence_type|avg_glucose_level| bmi| smoking_status|stroke|
+-----+------+---+------------+-------------+------------+-------------+--------------+-----------------+----+---------------+------+
| 9046|  Male| 67|           0|            1|         Yes|      Private|         Urban|           228.69|36.6|formerly smoked|     1|
|51676|Female| 61|           0|            0|         Yes|Self-employed|         Rural|           202.21|35.4|   never smoked|     1|
|31112|  Male| 80|           0|            1|         Yes|      Private|         Rural|           105.92|32.5|   never smoked|     1|
|60182|Female| 49|           0|            0|         Yes|      Private|         Urban|           171.23|34.4|         smokes|     1|
| 1665|Female| 79|           1|            0|         Yes|Self-employed|         Rural|           174.12|24.0|   never smoked|     1|
+-----+------+---+------------+-------------+------------+-------------+--------------+-----------------+----+---------------+------+
*/
// COMMAND ----------
val Array(train, test) = data.randomSplit(Array(0.8, 0.2))

val rForm = new RFormula()
val gbtClassifier = new GBTClassifier().setLabelCol("label").setFeaturesCol("features")
val stages = Array(rForm, gbtClassifier)
val pipeline = new Pipeline().setStages(stages)

                                    // COMMAND ----------
                                    val params = new ParamGridBuilder()
                                        .addGrid(rForm.formula, Array(
                                            "stroke ~.",
        "stroke ~. + work_type:hypertension"))
    .addGrid(gbtClassifier.maxDepth, Array(2, 5, 10))
    .addGrid(gbtClassifier.maxBins, Array(10, 20, 40))
    .addGrid(gbtClassifier.maxIter, Array(5, 10, 20))
    .build()

val evaluator = new BinaryClassificationEvaluator()
    .setMetricName("areaUnderROC")
    .setRawPredictionCol("prediction")
    .setLabelCol("label")

val tvs = new TrainValidationSplit()
    .setTrainRatio(0.75)
    .setEstimatorParamMaps(params)
    .setEstimator(pipeline)
    .setEvaluator(evaluator)

val tvsFitted = tvs.fit(train)
print(tvsFitted)
/*
TrainValidationSplitModel: uid=tvs_c010d8497e49, bestModel=pipeline_33eafee540fd, trainRatio=0.75
*/
val predict_out = tvsFitted.transform(test)
val out = predict_out   
    .select("label", "prediction")
    .rdd.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))

val mMetrics = new MulticlassMetrics(out)
val bEvaluator = new BinaryClassificationEvaluator().setLabelCol("label")

// confusionMatrix
println("Confusion matrix:")
println(mMetrics.confusionMatrix)

/*
Confusion matrix:
930.0  47.0
5.0    2.0
*/

// COMMAND ----------
val labels = mMetrics.labels

// Precision by label
labels.foreach { l =>
	println(s"Precision($l) = " + mMetrics.precision(l))
}
// Recall by label
labels.foreach { l =>
	println(s"Recall($l) = " + mMetrics.recall(l))
}

// False positive rate by label
labels.foreach { l =>
	println(s"FPR($l) = " + mMetrics.falsePositiveRate(l))
}

// F-measure by label
labels.foreach { l =>
	println(s"F1-Score($l) = " + mMetrics.fMeasure(l))
}

// accuracy
println(s"accuracy = " + mMetrics.accuracy)

/*
Precision(0.0) = 0.995
Precision(1.0) = 0.041
Recall(0.0) = 0.952
Recall(1.0) = 0.286
FPR(0.0) = 0.714
FPR(1.0) = 0.048
F1-Score(0.0) = 0.973
F1-Score(1.0) = 0.071
accuracy = 0.947
*/

// COMMAND ----------
def printlnMetric(metricName: String): Double = {
	val metrics = bEvaluator.setMetricName(metricName).evaluate(predict_out)
	metrics
}
println("Area Under ROC = " + printlnMetric("areaUnderROC")) 
println("Area Under PRC = "+ printlnMetric("areaUnderPR")) 

/*
Area Under ROC = 0.831
Area Under PRC = 0.179
*/
