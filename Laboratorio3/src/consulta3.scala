// Consulta 3: ¿Donde se da la mayor cantidad de ataques, en zonas urbanas o rurales?

///////////////// SCALA /////////////////
val load_data = sc.textFile("data-limpia.csv")
val data = load_data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
val stroke_by_zone = data.map(s => (s.split(",")(7) + '-' + s.split(",")(11), 1)).reduceByKey(_ + _)
stroke_by_zone.saveAsTextFile("output3")

///////////////// SPARK-SQL /////////////////
val load_config = spark.read.format("csv").option("header", "true").option("inferSchema", "true") 
val dfCSV = load_config.load("data-limpia.csv")
// print
dfCSV.show
dfCSV.printSchema()
// create table
dfCSV.createOrReplaceTempView("dfTable")
// sql query
val countSql = spark.sql("""
SELECT Residence_type, stroke, count(stroke) as count_stroke 
FROM dfTable
GROUP BY Residence_type, stroke
""")
countSql.show()