import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.RFormula

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

val supervised = new RFormula()
    .setFormula("stroke ~.")

val fittedRF = supervised.fit(data)
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

val Array(train, test) = preparedDF.randomSplit(Array(0.7, 0.3))

/*
val train: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [id: int, gender: string ... 12 more fields]
val test: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [id: int, gender: string ... 12 more fields]
*/
// Estimators

import org.apache.spark.ml.classification.GBTClassifier
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


import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.evaluation.MulticlassMetrics

val out = trainedModel.transform(test)
    .select("label", "prediction")
    .rdd.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))

val bMetrics = new BinaryClassificationMetrics(out)
val mMetrics = new MulticlassMetrics(out)

// confusionMatrix
println("Confusion matrix:")
println(mMetrics.confusionMatrix)

/*
Confusion matrix:
		1420.0  70.0
		6.0     2.0
*/

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

/*
Precision(0.0) = 0.9957924263674615
Precision(1.0) = 0.027777777777777776
Recall(0.0) = 0.9530201342281879
Recall(1.0) = 0.25
FPR(0.0) = 0.75
FPR(1.0) = 0.04697986577181208
F1-Score(0.0) = 0.9739368998628258
F1-Score(1.0) = 0.049999999999999996
*/

// Precision by threshold
val precision = bMetrics.precisionByThreshold
precision.foreach { case (t, p) =>
  println(s"Threshold: $t, Precision: $p")
}
// Recall by threshold
val recall = bMetrics.recallByThreshold
recall.foreach { case (t, r) =>
  println(s"Threshold: $t, Recall: $r")
}

// Precision-Recall Curve
val PRC = bMetrics.pr

// F-measure
val f1Score = bMetrics.fMeasureByThreshold
f1Score.foreach { case (t, f) =>
  println(s"Threshold: $t, F-score: $f, Beta = 1")
}
val beta = 0.5
val fScore = bMetrics.fMeasureByThreshold(beta)
f1Score.foreach { case (t, f) =>
  println(s"Threshold: $t, F-score: $f, Beta = 0.5")
}

// AUPRC
val auPRC = bMetrics.areaUnderPR
println("Área bajo la curva de precision-recall = " + auPRC)

// Compute thresholds used in ROC and PR curves
val thresholds = precision.map(_._1)

// ROC Curve
val roc = bMetrics.roc

// AUROC
val auROC = bMetrics.areaUnderROC
println("Area bajo ROC = " + auROC)

/*
Threshold: 1.0, Precision: 0.027777777777777776
Threshold: 0.0, Precision: 0.0053404539385847796
Threshold: 1.0, Recall: 0.25
Threshold: 0.0, Recall: 1.0
Threshold: 1.0, F-score: 0.049999999999999996, Beta = 1
Threshold: 0.0, F-score: 0.010624169986719787, Beta = 1
Threshold: 1.0, F-score: 0.049999999999999996, Beta = 0.5
Threshold: 0.0, F-score: 0.010624169986719787, Beta = 0.5
Área bajo la curva de precision-recall = 0.019363781338080404
Area bajo ROC = 0.6015100671140939
*/
