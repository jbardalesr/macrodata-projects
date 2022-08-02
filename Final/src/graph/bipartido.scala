import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.ListMap

case class Patient(
    id: Double,
    gender: Integer, 
    age: Double, 
    age_range: Integer,
    hypertension: Double, 
    heart_disease: Double,
    ever_married: Integer,
    work_type: Integer, 
    Residence_type: Integer, 
    avg_glucose_level: Double,
    bmi: Double,
    smoking_status: Integer, 
    stroke:Double)


class VertexProperty extends Serializable {}

case class PatientProperty(patientId: Double) extends VertexProperty
case class PredictStrokeProperty(stroke: Double) extends VertexProperty

val rawData = sc.textFile("/home/jc/Documentos/Macrodatos/Data/healthcare_clean_num.csv")
                .map(_.split(","))
                .map(line => Patient(line(0).toDouble,
                                line(1).toInt,
                                line(2).toDouble,
                                line(3).toInt,
                                line(4).toDouble,
                                line(5).toDouble,
                                line(6).toInt,
                                line(7).toInt,
                                line(8).toInt,
                                line(9).toDouble,
                                line(10).toDouble,
                                line(11).toInt,
                                line(12).toDouble))

val predictStroke = rawData.filter(_.smoking_status != 3).cache()

rawData.count()
predictStroke.count()

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
                map(_.stroke).
                distinct().
                zipWithIndex().
                map{case(stroke, zeroBasedIndex) => (stroke, zeroBasedIndex + startIndex )}

val predictStroke2VertexID = predictStrokeVertexRDD.collect().toMap

val predictStrokeVertex = predictStrokeVertexRDD.map{
                            case(stroke, index) => (index, PredictStrokeProperty(stroke))}.
                            asInstanceOf[RDD[(VertexId, VertexProperty)]]

// Creando los Edge
val bcPatient2VertexId = sc.broadcast(patient2VertexId)
val bcPredictStroke2VertexId = sc.broadcast(predictStroke2VertexID)

val edges = predictStroke.
            map(event => ((event.id, event.stroke), 1)).
            reduceByKey(_ + _).
            map{case((id, stroke), count) => (id, stroke, count)}.
            map{case(id, stroke, count) => Edge(
                bcPatient2VertexId.value(id),
                bcPredictStroke2VertexId.value(stroke),
                count
            )}

//  Ensamblado vertices y bordes
// val vertices = sc.union(patientVertex, predictStrokeVertex)
val vertices = predictStrokeVertex.union(patientVertex)

val graph = Graph(vertices, edges)

val connectedComponents = graph.connectedComponents()

connectedComponents.vertices.take(5)
connectedComponents.edges.take(5)

println("Numero total de artistas en el grafo: " + connectedComponents.edges.count());
println("Numero total de vertices en el grafo: " + connectedComponents.vertices.count());

val numVertices = (connectedComponents.vertices.map(pair => (pair._2,1))
                    .reduceByKey(_ + _)
                    .map(pair => pair._2))

println("Numero de conexiones: " + numVertices.count());

ListMap(numVertices.
       countByValue().toSeq.sortBy(_._1):_*).foreach(println)

// personas que tengan hypertension y menores a 60 años
val casePatient = rawData.
                    filter(event => event.hypertension == 1 && event.age < 60).cache().
                    map(_.id).
                    collect.
                    toSet

val bcCasePatient = sc.broadcast(casePatient)

val filteredGraph = graph.subgraph(vpred = {case(id, attr) => 
                            val isPatient = attr.isInstanceOf[PatientProperty]
                            val patient = if(isPatient) attr.asInstanceOf[PatientProperty] else null
                            !isPatient || (bcCasePatient.value contains patient.patientId)
                        })

val hipertension = filteredGraph.inDegrees.
                            takeOrdered(5)(scala.Ordering.by(-_._2))

graph.vertices.filter(_._1 == 5109).collect