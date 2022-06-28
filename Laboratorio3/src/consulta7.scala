// Databricks notebook source
// MAGIC %scala
// MAGIC val file_location = "/FileStore/tables/stroke_clean-3.csv"
// MAGIC 
// MAGIC val data = spark
// MAGIC   .read
// MAGIC   .option("inferSchema", "true")
// MAGIC   .option("header", "true")
// MAGIC   .csv(file_location)
// MAGIC 
// MAGIC data.printSchema()
// MAGIC data.show()

// COMMAND ----------

val file_location_demographic = "/FileStore/tables/stroke_demographic_clean-1.json"
val file_location_health = "/FileStore/tables/stroke_health_clean-1.json"

val df_Demo = spark.read.option("multiline","true").json(file_location_demographic)
val df_Salud = spark.read.option("multiline","true").json(file_location_health)

df_Demo.createOrReplaceTempView("dfTableDemo")
df_Salud.createOrReplaceTempView("dfTableSalud")

// COMMAND ----------

import org.apache.spark.sql.functions.{col,desc, round}

// spark
sc.setJobGroup("Consulta 7", "Spark Scala")

val health = data
  .filter(col("age") >= 18)
  .filter(col("avg_glucose_level") >=70 and col("avg_glucose_level") <= 126)
  .filter(col("hypertension").isin(0))
  .filter(col("heart_disease").isin(0))
  .filter(col("stroke").isin(1))
  .groupBy( "work_type", "smoking_status")
  .count()
  .sort(desc("count"))

health.show()

// COMMAND ----------

// sql
sc.setJobGroup("Consulta 7", "Spark Sql")

val health_sql = spark.sql("""
  SELECT A.work_type, B.smoking_status, count(1) as count
  FROM dfTableDemo A INNER JOIN dfTableSalud B ON A.id = B.id
  WHERE A.age >= 18  AND B.avg_glucose_level >= 70 AND  B.avg_glucose_level <= 126 AND B.hypertension == 0 AND B.heart_disease = 0 AND B.stroke = 1
  GROUP BY A.work_type, B.smoking_status
  ORDER BY count DESC
""")

health_sql.show()

// COMMAND ----------


