import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ 
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.collection.immutable.ListMap

sc.setLogLevel("ERROR")

val ssc = new StreamingContext(sc, Seconds(1))

val registros1 = ssc.socketTextStream("201.230.99.133",9999)
val registros2 = ssc.socketTextStream("192.168.1.13",9998)
val registros = registros1.union(registros2)

val saveModel = PipelineModel.load("./tmp/rf-model")
val saveModel = PipelineModel.load("/home/jc/Documentos/Macrodatos/macrodata-projects/Final/src/tmp/rf-model")
case class Patient(
    id: Double,
    gender: Integer, 
    age: Integer, 
    age_range: Integer,
    hypertension: Integer, 
    heart_disease: Integer,
    ever_married: Integer,
    work_type: Integer, 
    Residence_type: Integer, 
    avg_glucose_level: Double,
    bmi: Double,
    smoking_status: Integer, 
    prediction:Double)


class VertexProperty extends Serializable {}

case class PatientProperty(patientId: Double) extends VertexProperty
case class PredictStrokeProperty(stroke: Double) extends VertexProperty

registros.foreachRDD(rdd => {
    if(!rdd.isEmpty){
        var regDF = rdd.toDF("features")
        var regDF2 = regDF.select(split(col("features"),",").getItem(0).cast("String").as("id"),
                    split(col("features"),",").getItem(1).cast("Int").as("gender"),
                    split(col("features"),",").getItem(2).cast("Int").as("age"),
                    split(col("features"),",").getItem(3).cast("Int").as("age_range"),
                    split(col("features"),",").getItem(4).cast("Int").as("hypertension"),
                    split(col("features"),",").getItem(5).cast("Int").as("heart_disease"),
                    split(col("features"),",").getItem(6).cast("Int").as("ever_married"),
                    split(col("features"),",").getItem(7).cast("Int").as("work_type"),
                    split(col("features"),",").getItem(8).cast("Int").as("Residence_type"),
                    split(col("features"),",").getItem(9).cast("Double").as("avg_glucose_level"),
                    split(col("features"),",").getItem(10).cast("Double").as("bmi"),
                    split(col("features"),",").getItem(11).cast("Int").as("smoking_status"))
                    .drop("features")

        regDF2.coalesce(1)
        .write.option("header","true")
        .option("sep",",")
        .mode("append")
        .csv("./tmp/registros")

        val predictions = saveModel.transform(regDF2)

        predictions.show()

        val rawData = predictions.select("id", "gender", "age", "age_range" ,
                                        "hypertension", "heart_disease", "ever_married",
                                        "work_type", "Residence_type", "avg_glucose_level",
                                        "bmi", "smoking_status" , "prediction").rdd.
                                        map(line => Patient(line.getString(0).toDouble,
                                                                line.getInt(1),
                                                                line.getInt(2),
                                                                line.getInt(3),
                                                                line.getInt(4),
                                                                line.getInt(5),
                                                                line.getInt(6),
                                                                line.getInt(7),
                                                                line.getInt(8),
                                                                line.getDouble(9),
                                                                line.getDouble(10),
                                                                line.getInt(11),
                                                                line.getDouble(12)))


        val predictStroke = rawData.filter(_.smoking_status != 3)

        // Vertice de pacienteId
        val patientVertexRDD = rawData.
                                map(_.id).
                                distinct().
                                zipWithIndex()

        val patient2VertexId = patientVertexRDD.collect.toMap

        val patientVertex = patientVertexRDD.map{ 
                            case(id, index) => (index, PatientProperty(id))
                            }.asInstanceOf[RDD[(VertexId, VertexProperty)]]

        // Vertice de prediccion
        val startIndex = patient2VertexId.size

        val predictStrokeVertexRDD = predictStroke.
                        map(_.prediction).
                        distinct().
                        zipWithIndex().
                        map{case(stroke, zeroBasedIndex) => (stroke, zeroBasedIndex + startIndex )}

        val predictStroke2VertexID = predictStrokeVertexRDD.collect().toMap

        val predictStrokeVertex = predictStrokeVertexRDD.map{
                                    case(prediction, index) => (index, PredictStrokeProperty(prediction))}.
                                    asInstanceOf[RDD[(VertexId, VertexProperty)]]

        // Creando los Edge
        val bcPatient2VertexId = sc.broadcast(patient2VertexId)
        val bcPredictStroke2VertexId = sc.broadcast(predictStroke2VertexID)

        val edges = predictStroke.
                    map(event => ((event.id, event.prediction), 1)).
                    reduceByKey(_ + _).
                    map{case((id, prediction), count) => (id, prediction, count)}.
                    map{case(id, prediction, count) => Edge(
                        bcPatient2VertexId.value(id),
                        bcPredictStroke2VertexId.value(prediction),
                        count
                    )}

        //  Ensamblado vertices y bordes
        val vertices = predictStrokeVertex.union(patientVertex)

        val graph = Graph(vertices, edges)

        val connectedComponents = graph.connectedComponents()

        println("Numero total de artistas en el grafo: " + connectedComponents.edges.count());
        println("Numero total de vertices en el grafo: " + connectedComponents.vertices.count());

        println("Muestra de los primeros 5 vertices")
        connectedComponents.vertices.take(5).foreach(println)

        println("Muestra de las primeras 5 aristas")
        connectedComponents.edges.take(5).foreach(println)

        println("Muestra de los primeros 5 tripletes")
        connectedComponents.triplets.collect.take(5).foreach(println)

        val numVertices = (connectedComponents.vertices.map(pair => (pair._2,1))
                            .reduceByKey(_ + _)
                            .map(pair => pair._2))

        println("Numero de conexiones: " + numVertices.count());

        ListMap(numVertices.countByValue().toSeq.sortBy(_._1):_*).foreach(println)
        
        predictions
        .select("id", "prediction")
        .show()

        predictions.select("id", "prediction").coalesce(1)
        .write.option("header","true")
        .option("sep",",")
        .mode("append")
        .csv("./tmp/predicciones")
    }
})

ssc.start()    
ssc.awaitTermination()