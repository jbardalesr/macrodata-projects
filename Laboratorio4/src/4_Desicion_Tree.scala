import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{
  DecisionTreeClassificationModel,
  DecisionTreeClassifier
}

//RandomForestClassificationModel
//RandomForestClassifier

import org.apache.spark.ml.classification.
import org.apache.spark.ml.classification.

import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{
  IndexToString, 
  StringIndexer, 
  VectorIndexer
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import spark.sqlContext.implicits._
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.sql.functions._

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

/*
+-----+------+---+---------+------------+-------------+------------+---------+--------------+-----------------+----+--------------+------+
|   id|gender|age|age_range|hypertension|heart_disease|ever_married|work_type|Residence_type|avg_glucose_level| bmi|smoking_status|stroke|
+-----+------+---+---------+------------+-------------+------------+---------+--------------+-----------------+----+--------------+------+
| 9046|     0| 67|        4|           0|            1|           1|        3|             1|           228.69|36.6|             2|     1|
|51676|     1| 61|        4|           0|            0|           1|        4|             0|           202.21|23.6|             0|     1|
|31112|     0| 80|        4|           0|            1|           1|        3|             0|           105.92|32.5|             0|     1|
|56112|     0| 64|        4|           0|            1|           1|        3|             1|           191.61|37.5|             1|     1|
|34120|     0| 75|        4|           1|            0|           1|        3|             1|           221.29|25.8|             1|     1|
|27458|     1| 60|        4|           0|            0|           0|        3|             1|            89.22|37.8|             0|     1|
|25226|     0| 57|        3|           0|            1|           0|        2|             1|           217.08|23.6|             3|     1|
+-----+------+---+---------+------------+-------------+------------+---------+--------------+-----------------+----+--------------+------+
*/
patientDF.describe("age").show
/*
+-------+------------------+
|summary|               age|
+-------+------------------+
|  count|              1000|
|   mean|            48.134|
| stddev|22.849116528350194|
|    min|                 0|
|    max|                82|
+-------+------------------+
*/
patientDF.groupBy("stroke").avg("age").show
/*
+------+-----------------+
|stroke|         avg(age)|
+------+-----------------+
|     1|67.72690763052209|
|     0|41.63781624500666|
+------+-----------------+
*/
//                          0       1       2           3                   4               5               6           7                   8              9        10
val featureCols = Array("gender", "age", "age_range", "hypertension","heart_disease", "ever_married", "work_type", "Residence_type", "avg_glucose_level","bmi", "smoking_status")

//val featureCols = Array("age", "hypertension","heart_disease", "ever_married", "work_type", "Residence_type", "avg_glucose_level","bmi", "smoking_status")

//val featureCols = Array("age", "hypertension","heart_disease", "avg_glucose_level","Residence_type","bmi", "smoking_status")
// transformers
val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features") 
val labelIndexer = new StringIndexer().setInputCol("stroke").setOutputCol("label")
// estimator
val classifier =  new DecisionTreeClassifier().setImpurity("gini").setMaxDepth(12).setSeed(651).setMaxBins(40)
// pipeline
val pipeline = new Pipeline().setStages(Array(assembler, labelIndexer, classifier))
// test-train
val splitSeed = 50111
val Array(trainingData, testData) = patientDF.randomSplit(Array(0.80, 0.20), splitSeed)
// model
val model = pipeline.fit(trainingData)
val predictions = model.transform(testData)
predictions.show
predictions.select("label","rawPrediction", "probability","prediction").show()
/*
+-----+-------------+--------------------+----------+
|label|rawPrediction|         probability|prediction|
+-----+-------------+--------------------+----------+
|  0.0|   [12.0,0.0]|           [1.0,0.0]|       0.0|
|  1.0|    [0.0,3.0]|           [0.0,1.0]|       1.0|
|  1.0|    [0.0,9.0]|           [0.0,1.0]|       1.0|
|  1.0|   [26.0,0.0]|           [1.0,0.0]|       0.0|
|  1.0|    [5.0,1.0]|[0.83333333333333...|       0.0|
|  0.0|  [199.0,0.0]|           [1.0,0.0]|       0.0|
|  1.0|   [11.0,0.0]|           [1.0,0.0]|       0.0|
|  0.0|   [34.0,0.0]|           [1.0,0.0]|       0.0|
|  0.0|    [7.0,0.0]|           [1.0,0.0]|       0.0|
|  0.0|  [199.0,0.0]|           [1.0,0.0]|       0.0|
|  1.0|    [0.0,7.0]|           [0.0,1.0]|       1.0|
|  1.0|    [2.0,0.0]|           [1.0,0.0]|       0.0|
|  1.0|    [2.0,0.0]|           [1.0,0.0]|       0.0|
|  1.0|   [0.0,26.0]|           [0.0,1.0]|       1.0|
|  1.0|   [15.0,0.0]|           [1.0,0.0]|       0.0|
|  0.0|   [12.0,0.0]|           [1.0,0.0]|       0.0|
|  0.0|    [5.0,0.0]|           [1.0,0.0]|       0.0|
|  0.0|  [199.0,0.0]|           [1.0,0.0]|       0.0|
|  0.0|   [50.0,0.0]|           [1.0,0.0]|       0.0|
|  0.0|  [199.0,0.0]|           [1.0,0.0]|       0.0|
+-----+-------------+--------------------+----------+
*/
val binaryClassificationEvaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction")
val accuracy = binaryClassificationEvaluator.evaluate(predictions)
println("The accuracy before pipeline fitting: " + accuracy)
// accuracy: Double = 0.8479956466533649
def printlnMetric(metricName: String): Double = {
    val metrics = binaryClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
    metrics
}
println("Area Under ROC before tuning: " + printlnMetric("areaUnderROC"))
// Area Under ROC before tuning: 0.8479956466533648
println("Area Under PRC before tuning: "+ printlnMetric("areaUnderPR"))
// Area Under PRC before tuning: 0.6318387796865091

val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
println(s"Learned classification tree model:\n ${treeModel.toDebugString}")