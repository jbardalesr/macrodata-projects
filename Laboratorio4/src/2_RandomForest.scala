import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import spark.sqlContext.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.param.ParamMap 
import org.apache.spark.ml.PipelineModel

case class Patient(
    id: String,
    gender: Int,
    age: Int,
    age_range: Int,
    hypertension: Int,
    heart_disease: Int,
    ever_married: Int,
    work_type: Int,
    Residence_type: Int,
    avg_glucose_level: Double,
    bmi: Double,
    smoking_status: Int,
    stroke: Int
)

def parsePatient(line: Patient): Patient = {
    Patient(
        line.id,
        line.gender,
        line.age,
        line.age_range,
        line.hypertension,
        line.heart_disease,
        line.ever_married,
        line.work_type,
        line.Residence_type,
        line.avg_glucose_level,
        line.bmi,
        line.smoking_status,
        line.stroke
    )
}

def parseRDD(rdd: RDD[String]): RDD[Patient]  = {
    rdd.map(_.split(",")).map(line => Patient(line(0),
        line(1).toInt,
        line(2).toInt,
        line(3).toInt,
        line(4).toInt,
        line(5).toInt,
        line(6).toInt,
        line(7).toInt,
        line(8).toInt,
        line(9).toDouble,
        line(10).toDouble,
        line(11).toInt,
        line(12).toInt))
}

// CARGANDO LOS DATOS
val patientRDD = parseRDD(sc.textFile("data_clean_numerico.csv")).map(parsePatient)
val patientDF = patientRDD.toDF().cache()


// specify the exact fraction desired from each key
val fractions = Map(0 -> 0.5, 1 -> 0.5)

// Get an approximate sample from each stratum
val approxSample = patientDF.sampleBy("stroke", fractions, 1000)

patientDF.show()

// PROCESOS DEL PIPELINE
val featureCols = Array("gender", "age", "age_range", "hypertension","heart_disease", "ever_married", "work_type", "Residence_type", "avg_glucose_level","bmi", "smoking_status")
// transformers
val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features") 
val labelIndexer = new StringIndexer().setInputCol("stroke").setOutputCol("label")
// estimator
val classifier = new RandomForestClassifier().setImpurity("gini").setMaxDepth(30).setNumTrees(30).setFeatureSubsetStrategy("auto").setSeed(1234567).setMinInfoGain(0.001)
// pipeline
val pipeline = new Pipeline().setStages(Array(assembler, labelIndexer, classifier))
// test-train
val splitSeed = 5043
val Array(trainingData, testData) = patientDF.randomSplit(Array(0.80, 0.20), splitSeed)

// ENTRENAMIENTO DEL MODELO
val model = pipeline.fit(trainingData)
val predictions = model.transform(testData)

// PREDICCIÓN DEL MODELO
predictions.select("label","rawPrediction", "probability","prediction").show()
/*
+-----+--------------------+--------------------+----------+
|label|       rawPrediction|         probability|prediction|
+-----+--------------------+--------------------+----------+
|  0.0|[21.0836108028116...|[0.70278702676038...|       0.0|
|  0.0|[26.6216135383553...|[0.88738711794517...|       0.0|
|  0.0|[29.6329682633722...|[0.98776560877907...|       0.0|
|  0.0|[14.1666666666666...|[0.47222222222222...|       1.0|
|  0.0|[29.6261189483037...|[0.98753729827679...|       0.0|
|  0.0|[13.9503546099290...|[0.46501182033096...|       1.0|
|  1.0|         [17.4,12.6]|         [0.58,0.42]|       0.0|
|  1.0|         [18.0,12.0]|           [0.6,0.4]|       0.0|
|  1.0|[29.5071464763781...|[0.98357154921260...|       0.0|
|  0.0|[28.6675691915459...|[0.95558563971819...|       0.0|
|  1.0|          [3.0,27.0]|           [0.1,0.9]|       1.0|
|  0.0|[28.5472888580623...|[0.95157629526874...|       0.0|
|  0.0|[26.2418235513920...|[0.87472745171306...|       0.0|
|  1.0|         [15.5,14.5]|[0.51666666666666...|       0.0|
|  0.0|[21.5246636672222...|[0.71748878890740...|       0.0|
|  1.0|       [12.75,17.25]|       [0.425,0.575]|       1.0|
|  1.0|         [17.0,13.0]|[0.56666666666666...|       0.0|
|  0.0|         [13.0,17.0]|[0.43333333333333...|       1.0|
|  0.0|[29.6434945791616...|[0.98811648597205...|       0.0|
|  1.0|[22.1410313901345...|[0.73803437967115...|       0.0|
+-----+--------------------+--------------------+----------+
*/

// EVALUANDO LAS MÉTRICAS
val predictionAndLabels =  predictions.select("label", "prediction").rdd.map( l => (l(0), l(1)) )
val metrics = new MulticlassMetrics(predictionAndLabels)

// Matriz de confusión
println(s"Confusion matrix:\n${metrics.confusionMatrix}")
/*
Confusion matrix:
138.0  20.0
11.0   17.0
*/

// Precisión
val labels = metrics.labels
labels.foreach { l =>
  println(s"Precision($l) = " + metrics.precision(l))
}
/*
Precision(0.0) = 0.9261744966442953
Precision(1.0) = 0.4594594594594595
*/

// Recall
labels.foreach { l =>
  println(s"Recall($l) = ${metrics.recall(l)}")
}
/*
Recall(0.0) = 0.8734177215189873
Recall(1.0) = 0.6071428571428571
*/

// Acuraccy
println(s"Accuracy = ${metrics.accuracy}")
// Accuracy = 0.8333333333333334

// F1-score
labels.foreach { l =>
  println(s"F1-Score($l) = " + metrics.fMeasure(l))
}
/*
F1-Score(0.0) = 0.8990228013029316
F1-Score(1.0) = 0.5230769230769231
*/


// SUMMARY
val precision = metrics.precision(_)
val recall = metrics.recall(_)
val accuracy = metrics.accuracy
val label_value = 0.0 // label_value
println(s"Precision: ${precision(label_value)} Recall: ${recall(label_value)} Accuracy: ${accuracy}")
// Precision: 0.9261744966442953 Recall: 0.8734177215189873 Accuracy: 0.8333333333333334    // label = 0
// Precision: 0.4594594594594595 Recall: 0.6071428571428571 Accuracy: 0.8333333333333334    // label = 1

// AU_ROC ===========================
val binaryClassificationEvaluator = new BinaryClassificationEvaluator().setLabelCol("label")
def printlnMetric(metricName: String): Double = {
    val metrics = binaryClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
    metrics
}
println("Area Under ROC before tuning: " + printlnMetric("areaUnderROC"))
// Area Under ROC before tuning: 0.8431888264103032
println("Area Under PRC before tuning: "+ printlnMetric("areaUnderPR"))
// Area Under PRC before tuning: 0.629339861363011


// CROSS VALIDATION ================
val paramGrid: Array[ParamMap] = new ParamGridBuilder().addGrid(classifier.maxDepth, Array(10, 20, 25, 30)).addGrid(classifier.numTrees, Array(50, 60, 70, 80)).build()

val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(new BinaryClassificationEvaluator).setEstimatorParamMaps(paramGrid).setNumFolds(5).setParallelism(2)

val cvModel = cv.fit(trainingData)
val pred = cvModel.transform(testData)

val predAndLabels =  pred.select("prediction", "label").rdd.map( l => (l(1),l(0)) )

val metrics = new MulticlassMetrics(predAndLabels)

// Matriz de confusión
println(s"Confusion matrix:\n${metrics.confusionMatrix}")
/*
Confusion matrix:
141.0  18.0
8.0    19.0
*/

// Precisión
val labels = metrics.labels
labels.foreach { l =>
  println(s"Precision($l) = " + metrics.precision(l))
}
/*
Precision(0.0) = 0.9463087248322147
Precision(1.0) = 0.5135135135135135
*/

// Recall
labels.foreach { l =>
  println(s"Recall($l) = ${metrics.recall(l)}")
}
/*
Recall(0.0) = 0.8867924528301887
Recall(1.0) = 0.7037037037037037
*/

// Acuraccy
println(s"Accuracy = ${metrics.accuracy}")
// Accuracy = 0.8602150537634409

// F1-score
labels.foreach { l =>
  println(s"F1-Score($l) = " + metrics.fMeasure(l))
}
/*
F1-Score(0.0) = 0.9155844155844155
F1-Score(1.0) = 0.59375
*/


// SUMMARY
val precision = metrics.precision(_)
val recall = metrics.recall(_)
val accuracy = metrics.accuracy
val label_value = 0.0 // label_value
println(s"Precision: ${precision(label_value)} Recall: ${recall(label_value)} Accuracy: ${accuracy}")
// Precision: 0.9463087248322147 Recall: 0.8867924528301887 Accuracy: 0.8602150537634409    // label = 0
// Precision: 0.5135135135135135 Recall: 0.7037037037037037 Accuracy: 0.8602150537634409    // label = 1

// AU_ROC ===========================
val binaryClassificationEvaluator = new BinaryClassificationEvaluator().setLabelCol("label")
def printlnMetric(metricName: String): Double = {
    val metrics = binaryClassificationEvaluator.setMetricName(metricName).evaluate(pred)
    metrics
}
println("Area Under ROC after tuning: " + printlnMetric("areaUnderROC"))
// Area Under ROC after tuning: 0.8610556865590421
println("Area Under PRC after tuning: "+ printlnMetric("areaUnderPR"))
// Area Under PRC after tuning: 0.666848889344384

cvModel.bestModel.
        asInstanceOf[PipelineModel].
        stages(2).
        asInstanceOf[RandomForestClassificationModel].
        extractParamMap()

/*
rfc_52286481d95c-maxDepth: 10,
rfc_52286481d95c-numTrees: 70,
*/