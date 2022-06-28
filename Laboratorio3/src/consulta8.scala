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

// spark
sc.setJobGroup("Consulta 8_1", "Spark Scala")

val urban_stroke = data
  .filter(col("Residence_type").isin("Urban"))
  .groupBy("stroke", "work_type")
  .avg("age")
  .select(col("stroke"), col("work_type"),round(col("avg(age)"),1))
  .withColumnRenamed("round(avg(age), 1)", "avg_age")
  .sort("stroke")

urban_stroke.show()

// COMMAND ----------

// sql
sc.setJobGroup("Consulta 8_1", "Spark Sql")

val urban_stroke_sql = spark.sql("""
  SELECT B.stroke, A.work_type, ROUND(AVG(A.age),1) AS avg_age
  FROM dfTableDemo A INNER JOIN dfTableSalud B ON A.id = B.id
  WHERE A.Residence_type = "Urban" 
  GROUP BY A.work_type, B.stroke
  ORDER BY stroke 
""")

urban_stroke_sql.show()

// COMMAND ----------

// spark
sc.setJobGroup("Consulta 8_2", "Spark Scala")

val rural_stroke = data
  .filter(col("Residence_type").isin("Rural"))
  .groupBy("stroke", "work_type")
  .avg("age")
  .select(col("stroke"), col("work_type"),round(col("avg(age)"),1))
  .withColumnRenamed("round(avg(age), 1)", "avg_age")
  .sort("stroke")

rural_stroke.show()

// COMMAND ----------

// sql
sc.setJobGroup("Consulta 8_2", "Spark Sql")

val urban_stroke_sql = spark.sql("""
  SELECT B.stroke, A.work_type, ROUND(AVG(A.age),1) AS avg_age
  FROM dfTableDemo A INNER JOIN dfTableSalud B ON A.id = B.id
  WHERE A.Residence_type = "Rural" 
  GROUP BY A.work_type, B.stroke
  ORDER BY stroke
""")

urban_stroke_sql.show()

// COMMAND ----------


