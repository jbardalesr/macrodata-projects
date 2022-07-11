import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.param.ParamMap 
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

// ============ Lectura de datos =============

val data_csv = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("data_clean_numerico.csv")

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

val patientRDD = parseRDD(sc.textFile("data_clean_numerico.csv")).map(parsePatient)
val patientDF = patientRDD.toDF().cache()
patientDF.show()

// ============ Pipeline =============

val featureCols = Array("gender", "age", "age_range", "hypertension","heart_disease", "ever_married", "work_type", "Residence_type", "avg_glucose_level","bmi", "smoking_status")
val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features") 
val labelIndexer = new StringIndexer().setInputCol("stroke").setOutputCol("label")
val lsvc = new LinearSVC().setMaxIter(15).setRegParam(0.01)

val pipeline = new Pipeline().setStages(Array(assembler,labelIndexer,lsvc))
val splitSeed = 5043
val Array(trainingData, testData) = patientDF.randomSplit(Array(0.80, 0.20),splitSeed)

// ============ Entrenamiento =============

val model = pipeline.fit(trainingData)
val predictions = model.transform(testData)


// ============ Evaluación =============

val predictionAndLabels =  predictions.select("prediction","label").rdd.map( l => (l(0),l(1)) )

val metrics = new MulticlassMetrics(predictionAndLabels)

// Confusion matrix
println("Confusion matrix:")
println(metrics.confusionMatrix)


val precision = metrics.precision(_)
val recall = metrics.recall(_)
val accuracy = metrics.accuracy
val a = 0.0
println(s"Precision: ${precision(a)} Recall: ${recall(a)} Accuracy: ${accuracy}")
//Precision: 0.8402366863905325 Recall: 0.9530201342281879 Accuracy: 0.8172043010752689
val binaryClassificationEvaluator = new BinaryClassificationEvaluator().setLabelCol("label")
def printlnMetric(metricName: String): Double = {
val metrics = binaryClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
metrics
}
println("Area Under ROC" + printlnMetric("areaUnderROC")) //Area Under ROC0.8022855069834938
println("Area Under PRC"+ printlnMetric("areaUnderPR")) //Area Under PRC0.5411919541043102

// ============ CROSS VALIDATION =============


val paramGrid: Array[ParamMap] = new ParamGridBuilder().
  addGrid(lsvc.maxIter,Array(5,10,12,15,17,20)).
  addGrid(lsvc.regParam,Array(0.1,0.01)).
  build()

val cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(new BinaryClassificationEvaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(3)  // Use 3+ in practice
  .setParallelism(2)  // Evaluate up to 2 parameter settings in parallel

// Run cross-validation, and choose the best set of parameters.
val cvModel = cv.fit(trainingData)
val pred = cvModel.transform(testData)

val predAndLabels =  pred.select("prediction","label").rdd.map( l => (l(0),l(1)) )

val metrics = new MulticlassMetrics(predAndLabels)

// Confusion matrix
println("Confusion matrix:")
println(metrics.confusionMatrix)


val precision = metrics.precision(_)
val recall = metrics.recall(_)
val accuracy = metrics.accuracy
val a = 0.0
println(s"Precision: ${precision(a)} Recall: ${recall(a)} Accuracy: ${accuracy}")
//Precision: 0.8255813953488372 Recall: 0.9530201342281879 Accuracy: 0.8010752688172043


def printlnMetric(metricName: String): Double = {
val metrics = binaryClassificationEvaluator.setMetricName(metricName).evaluate(pred)
metrics
}
println("Area Under ROC" + printlnMetric("areaUnderROC")) //Area Under ROC0.791764919281698
println("Area Under PRC"+ printlnMetric("areaUnderPR")) //Area Under PRC0.5421393776288758