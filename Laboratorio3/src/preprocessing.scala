// Databricks notebook source
// MAGIC %scala
// MAGIC val file_location = "/FileStore/tables/healthcare_dataset_stroke_data.csv"
// MAGIC 
// MAGIC val data = spark
// MAGIC   .read
// MAGIC   .option("inferSchema", "true")
// MAGIC   .option("header", "true")
// MAGIC   .csv(file_location)
// MAGIC 
// MAGIC data.printSchema()

// COMMAND ----------

data.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Se elimina el valor "Other" del campo gender

// COMMAND ----------

import org.apache.spark.sql.functions.{expr, col, column, round}

// mostrar los valores distintos para gender
data.select("gender").distinct().show()

// COMMAND ----------

val data_gender = data.filter("gender != 'Other'")

data_gender.select("gender").distinct().show()

// COMMAND ----------

case class stroke(id: Integer, gender: String, age: Double,
                 hypertension: Integer, heart_disease: Integer,
                 ever_married: String, work_type: String,
                 Residence_type: String, avg_glucose_level: Double,
                 bmi: String, smoking_status: String, stroke:Integer)

// COMMAND ----------

// import org.apache.spark.sql.functions.map
// edades pasar a entero
val data_stroke = data_gender.as[stroke]
val data_map = data_stroke.map(row =>{
  val ageInt = row.age.toInt
  (row.id, row.gender, ageInt, row.hypertension,
  row.heart_disease, row.ever_married, row.work_type,
  row.Residence_type, row.avg_glucose_level, row.bmi, 
   row.smoking_status, row.stroke)})

val data_age = data_map.toDF("id", "gender", "age", "hypertension", "heart_disease", "ever_married", "work_type",	"Residence_type",
                           "avg_glucose_level",	"bmi","smoking_status", "stroke").as[stroke]
data_age.show()

// COMMAND ----------

import org.apache.spark.sql.functions.regexp_replace

val bmi_avg = data_age.agg("bmi" -> "avg").as[Double].collect.head
val fillColValues = Map("bmi" -> bmi_avg)
val data_clean = data_age.select(
  col("id"), col("gender"), col("age"), col("hypertension"), col("heart_disease"),
  col("ever_married"), col("work_type"), col("Residence_type"), col("avg_glucose_level"),
  round(regexp_replace(col("bmi"), "N/A", bmi_avg.toString),1).alias("bmi"), col("smoking_status"), col("stroke")
  )

data_clean.show()

// COMMAND ----------

data_clean.write.csv("/tmp/spark_output/stroke.csv")

// COMMAND ----------

data_clean.write.json("stroke_clean")

// COMMAND ----------

display(data_clean)

// COMMAND ----------


