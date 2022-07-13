
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.{expr, col, column, round}
import scala.util.Random

// COMMAND ----------
// val file_location = "../Data/healthcare_dataset_stroke_data.csv"
val file_location = "/home/jc/Documentos/Macrodatos/macrodata-projects/Laboratorio4/Data/healthcare_dataset_stroke_data.csv"

// COMMAND ----------
val data = spark
  .read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv(file_location).cache()

// COMMAND ----------
data.show(5)

/*
+-----+------+----+------------+-------------+------------+-------------+--------------+-----------------+----+---------------+------+
|   id|gender| age|hypertension|heart_disease|ever_married|    work_type|Residence_type|avg_glucose_level| bmi| smoking_status|stroke|
+-----+------+----+------------+-------------+------------+-------------+--------------+-----------------+----+---------------+------+
| 9046|  Male|67.0|           0|            1|         Yes|      Private|         Urban|           228.69|36.6|formerly smoked|     1|
|51676|Female|61.0|           0|            0|         Yes|Self-employed|         Rural|           202.21| N/A|   never smoked|     1|
|31112|  Male|80.0|           0|            1|         Yes|      Private|         Rural|           105.92|32.5|   never smoked|     1|
|60182|Female|49.0|           0|            0|         Yes|      Private|         Urban|           171.23|34.4|         smokes|     1|
| 1665|Female|79.0|           1|            0|         Yes|Self-employed|         Rural|           174.12|  24|   never smoked|     1|
+-----+------+----+------------+-------------+------------+-------------+--------------+-----------------+----+---------------+------+
*/

// COMMAND ----------
data.printSchema()

/*
root
 |-- id: integer (nullable = true)
 |-- gender: string (nullable = true)
 |-- age: double (nullable = true)
 |-- hypertension: integer (nullable = true)
 |-- heart_disease: integer (nullable = true)
 |-- ever_married: string (nullable = true)
 |-- work_type: string (nullable = true)
 |-- Residence_type: string (nullable = true)
 |-- avg_glucose_level: double (nullable = true)
 |-- bmi: string (nullable = true)
 |-- smoking_status: string (nullable = true)
 |-- stroke: integer (nullable = true)
 */

// COMMAND ----------


// mostrar los valores distintos para gender
data.select("gender").distinct().show()

/*
+------+
|gender|
+------+
|Female|
| Other|
|  Male|
+------+
*/

// filtramos para gender "Other"
val other = data.filter("gender == 'Other'")
other.show()

/*
+-----+------+----+------------+-------------+------------+---------+--------------+-----------------+----+---------------+------+
|   id|gender| age|hypertension|heart_disease|ever_married|work_type|Residence_type|avg_glucose_level| bmi| smoking_status|stroke|
+-----+------+----+------------+-------------+------------+---------+--------------+-----------------+----+---------------+------+
|56156| Other|26.0|           0|            0|          No|  Private|         Rural|           143.33|22.4|formerly smoked|     0|
+-----+------+----+------------+-------------+------------+---------+--------------+-----------------+----+---------------+------+
*/


// filtramos para gender != Other 
val data_gender = data.filter("gender != 'Other'")

data_gender.select("gender").distinct().show()

/*
+------+
|gender|
+------+
|Female|
|  Male|
+------+
*/

// edades pasar a entero

case class stroke(id: Integer, gender: String, age: Double,
                 hypertension: Integer, heart_disease: Integer,
                 ever_married: String, work_type: String,
                 Residence_type: String, avg_glucose_level: Double,
                 bmi: String, smoking_status: String, stroke:Integer)


val data_stroke = data_gender.as[stroke]

val data_map = data_stroke.map(row =>{
  
  val ageInt = row.age.round

  (row.id, row.gender, ageInt, row.hypertension,
  row.heart_disease, row.ever_married, row.work_type,
  row.Residence_type, row.avg_glucose_level, row.bmi, 
   row.smoking_status, row.stroke)
   })

val data_age = data_map.toDF("id", "gender", "age", "hypertension", "heart_disease", "ever_married", "work_type",	"Residence_type",
                           "avg_glucose_level",	"bmi","smoking_status", "stroke").as[stroke]

data_age.show(5)

/*
+-----+------+---+------------+-------------+------------+-------------+--------------+-----------------+----+---------------+------+
|   id|gender|age|hypertension|heart_disease|ever_married|    work_type|Residence_type|avg_glucose_level| bmi| smoking_status|stroke|
+-----+------+---+------------+-------------+------------+-------------+--------------+-----------------+----+---------------+------+
| 9046|  Male| 67|           0|            1|         Yes|      Private|         Urban|           228.69|36.6|formerly smoked|     1|
|51676|Female| 61|           0|            0|         Yes|Self-employed|         Rural|           202.21| N/A|   never smoked|     1|
|31112|  Male| 80|           0|            1|         Yes|      Private|         Rural|           105.92|32.5|   never smoked|     1|
|60182|Female| 49|           0|            0|         Yes|      Private|         Urban|           171.23|34.4|         smokes|     1|
| 1665|Female| 79|           1|            0|         Yes|Self-employed|         Rural|           174.12|  24|   never smoked|     1|
+-----+------+---+------------+-------------+------------+-------------+--------------+-----------------+----+---------------+------+
*/

val bmi_avg = data_age.agg("bmi" -> "avg").as[Double].collect.head
val bmi_std = data_age.agg("bmi" -> "std").as[Double].collect.head
val min_ = bmi_avg - bmi_std
val max_ = bmi_avg + bmi_std

def random_bmi(min:Double, max:Double): String = {
  val rand = Random.nextDouble()*(max - min) + min
  return rand.toString()
}

val fillColValues = Map("bmi" -> bmi_avg)
val data_bmi = data_age.select(
  col("id"), col("gender"), col("age"), col("hypertension"), col("heart_disease"),
  col("ever_married"), col("work_type"), col("Residence_type"), col("avg_glucose_level"),
  round(regexp_replace(col("bmi"), "N/A", random_bmi(min_, max_)),1).alias("bmi"), col("smoking_status"), col("stroke")
  )

data_bmi.show(5)

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

// Exportar a un csv

data_bmi.coalesce(1)
  .write.option("header","true")
  .option("sep",",")
  .mode("overwrite")
  .csv("/home/jc/Documentos/Macrodatos/macrodata-projects/Laboratorio4/Data/stroke_clean.csv")

// transformar campos categoria a numericas

val data_bmi_stroke = data_bmi.as[stroke]

val parseGender = Map("Male" -> 0, "Female" -> 1)
val parseMarried = Map("No" -> 0, "Yes" -> 1)
val parseWordType = Map("Never_worked" -> 0, "children" -> 1, "Govt_job" -> 2, "Private" -> 3 , "Self-employed" -> 4)
val parseResidenceType = Map("Rural" -> 0, "Urban" -> 1)
val parseSmokingStatus = Map("never smoked" -> 0, "smokes" -> 1, "formerly smoked" -> 2, "Unknown" -> 3)


val data_numeric = data_bmi_stroke.map(row =>{
  var range = 0
  if (row.age <=11) range = 0
  if ((row.age > 11) && (row.age <=18) )range = 1
  if ((row.age > 18) && (row.age <=26) )range = 2
  if ((row.age > 26) && (row.age <=59)) range = 3
  if (row.age > 59) range = 4
  
  (row.id.toDouble, parseGender(row.gender), row.age, range,  row.hypertension.toDouble,
  row.heart_disease.toDouble, parseMarried(row.ever_married), parseWordType(row.work_type),
  parseResidenceType(row.Residence_type), row.avg_glucose_level, row.bmi.toDouble, 
  parseSmokingStatus(row.smoking_status), row.stroke.toDouble)})

case class strokeII(id: Double, gender: Integer, age: Double, 
                  age_range: Integer,hypertension: Double, heart_disease: Double,
                  ever_married: Integer, work_type: Integer, Residence_type: Integer, 
                  avg_glucose_level: Double,bmi: Double, smoking_status: Integer, stroke:Double)

val data_clean = data_numeric.toDF("id", "gender", "age", "age_range", "hypertension", "heart_disease", "ever_married", "work_type",	"Residence_type",
                           "avg_glucose_level",	"bmi","smoking_status", "stroke").as[strokeII]
data_clean.show(5)

/*
+-------+------+----+---------+------------+-------------+------------+---------+--------------+-----------------+----+--------------+------+
|     id|gender| age|age_range|hypertension|heart_disease|ever_married|work_type|Residence_type|avg_glucose_level| bmi|smoking_status|stroke|
+-------+------+----+---------+------------+-------------+------------+---------+--------------+-----------------+----+--------------+------+
| 9046.0|     0|67.0|        4|         0.0|          1.0|           1|        3|             1|           228.69|36.6|             2|   1.0|
|51676.0|     1|61.0|        4|         0.0|          0.0|           1|        4|             0|           202.21|35.4|             0|   1.0|
|31112.0|     0|80.0|        4|         0.0|          1.0|           1|        3|             0|           105.92|32.5|             0|   1.0|
|60182.0|     1|49.0|        3|         0.0|          0.0|           1|        3|             1|           171.23|34.4|             1|   1.0|
| 1665.0|     1|79.0|        4|         1.0|          0.0|           1|        4|             0|           174.12|24.0|             0|   1.0|
+-------+------+----+---------+------------+-------------+------------+---------+--------------+-----------------+----+--------------+------+
*/

// Exportar a un csv

data_bmi.coalesce(1)
  .write.option("header","true")
  .option("sep",",")
  .mode("overwrite")
  .csv("/home/jc/Documentos/Macrodatos/macrodata-projects/Laboratorio4/Data/data_clean_numerico.csv")