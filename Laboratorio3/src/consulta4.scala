// Consulta 4: ¿Edades promedio de los que sufrieron ataque según el tipo de empleo?

///////////////// SCALA /////////////////
val load_data = sc.textFile("data-limpia.csv")
val data = load_data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
// filtra unicamente aquellos con ataque stroke=1
val stroke_data = data.filter(x => x.split(",")(11).equals("1"))
// concatena work_type con las edades
val type_stroke = stroke_data.map(s => s.split(",")(6) + ',' + s.split(",")(2))
// separa work_type y edades para sumar las edades
val mean_by_wtype = type_stroke.map(row => (row.split(",")(0), row.split(",")(1).toFloat))
// suma y cuenta cada work_type
val reduce_sum_count = mean_by_wtype.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
// divide entre la suma y la cantidad de elementos en cada work_type
val mean_age = reduce_sum_count.map(row => (row._1, row._2._1/row._2._2))
mean_age.saveAsTextFile("output4")

///////////////// SPARK-SQL /////////////////
val load_config = spark.read.format("csv").option("header", "true").option("inferSchema", "true") 
val dfCSV_Demo = load_config.load("stroke_demografico.csv")
val dfCSV_Salud = load_config.load("stroke_salud.csv")

// val dfJson = spark.read.json("stroke_demografico.json")
dfCSV_Demo.createOrReplaceTempView("dfTableDemo")
dfCSV_Salud.createOrReplaceTempView("dfTableSalud")

val meanSql = spark.sql("""
SELECT work_type, avg(age) as mean_age 
FROM dfTableDemo JOIN dfTableSalud ON dfTableDemo.id = dfTableSalud.id 
WHERE stroke == 1 
GROUP BY work_type
""")
meanSql.show()