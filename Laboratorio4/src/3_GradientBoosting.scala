import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.mllib.evaluation.MulticlassMetrics

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
 
println(data.filter("stroke == '0'").count())
println(data.filter("stroke == '1'").count())
// COMMAND ----------
val stroke = new RFormula()
                    .setFormula("stroke ~.")

val fittedRF = stroke.fit(data)
val preparedDF = fittedRF.transform(data)

preparedDF.show(5)

/*
----+------+---+------------+-------------+------------+-------------+--------------+-----------------+----+---------------+------+--------------------+-----+
|   id|gender|age|hypertension|heart_disease|ever_married|    work_type|Residence_type|avg_glucose_level| bmi| smoking_status|stroke|            features|label|
+-----+------+---+------------+-------------+------------+-------------+--------------+-----------------+----+---------------+------+--------------------+-----+
| 9046|  Male| 67|           0|            1|         Yes|      Private|         Urban|           228.69|36.6|formerly smoked|     1|(16,[0,2,4,5,6,10...|  1.0|
|51676|Female| 61|           0|            0|         Yes|Self-employed|         Rural|           202.21|35.4|   never smoked|     1|(16,[0,1,2,5,7,11...|  1.0|
|31112|  Male| 80|           0|            1|         Yes|      Private|         Rural|           105.92|32.5|   never smoked|     1|(16,[0,2,4,5,6,11...|  1.0|
|60182|Female| 49|           0|            0|         Yes|      Private|         Urban|           171.23|34.4|         smokes|     1|(16,[0,1,2,5,6,10...|  1.0|
| 1665|Female| 79|           1|            0|         Yes|Self-employed|         Rural|           174.12|24.0|   never smoked|     1|(16,[0,1,2,3,5,7,...|  1.0|
+-----+------+---+------------+-------------+------------+-------------+--------------+-----------------+----+---------------+------+--------------------+-----+
*/

// split a la data

// COMMAND ----------
val Array(train, test) = preparedDF.randomSplit(Array(0.8, 0.2))
println(train.filter("stroke == '1'").count())
println(test.filter("stroke == '1'").count())
/*
val train: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [id: int, gender: string ... 12 more fields]
val test: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [id: int, gender: string ... 12 more fields]
*/

// Estimators
val gbtClassifier = new GBTClassifier().setLabelCol("label").setFeaturesCol("features")
// println(gbtClassifier.explainParams())
val trainedModel = gbtClassifier.fit(train)

/*
val trainedModel: org.apache.spark.ml.classification.GBTClassificationModel = GBTClassificationModel: uid = gbtc_97b9f7ffea1d, numTrees=20, numClasses=2, numFeatures=16
*/

trainedModel.transform(train).select("label", "prediction").show(5)

/*
+-----+----------+
|label|prediction|
+-----+----------+
|  0.0|       0.0|
|  0.0|       0.0|
|  0.0|       0.0|
|  0.0|       0.0|
|  0.0|       0.0|
+-----+----------+
*/

// COMMAND ----------
trainedModel.transform(test).select("label", "prediction").show(5)
/*
+-----+----------+
|label|prediction|
+-----+----------+
|  0.0|       0.0|
|  0.0|       0.0|
|  0.0|       0.0|
|  0.0|       0.0|
|  0.0|       0.0|
+-----+----------+
*/
// Evaluatuion Metrics
// COMMAND ----------

val predict_out = trainedModel.transform(test)
val out = predict_out
    .select("label", "prediction")
    .rdd.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
val mMetrics = new MulticlassMetrics(out)
val bEvaluator = new BinaryClassificationEvaluator().setLabelCol("label")

// confusionMatrix
println("Confusion matrix:")
println(mMetrics.confusionMatrix.toString)

/*
Confusion matrix:
        122.0  22.0
        19.0    27.0
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
Precision(0.0) = 0.847
Precision(1.0) = 0.587
Recall(0.0) = 0.865
Recall(1.0) = 0.551
FPR(0.0) = 0.449
FPR(1.0) = 0.135
F1-Score(0.0) = 0.856
F1-Score(1.0) = 0.568
accuracy = 0.784
*/


// COMMAND ----------
def printlnMetric(metricName: String): Double = {
	val metrics = bEvaluator.setMetricName(metricName).evaluate(predict_out)
	metrics
}
println("Area Under ROC = " + printlnMetric("areaUnderROC")) 
println("Area Under PRC = "+ printlnMetric("areaUnderPR")) 

/*
Area Under ROC = 0.838
Area Under PRC = 0.561
*/