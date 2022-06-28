val load_data = sc.textFile("data/stroke_clean.csv")
val data = load_data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

val sufrieronAtaque =  data.filter(row => row.split(",")(11)=="1")
sufrieronAtaque.count()
val nosufrieronAtaque =  data.filter(row => row.split(",")(11)=="0")
nosufrieronAtaque.count()

val gendersNo = nosufrieronAtaque.map(row => row.split(",")(1) -> 1).reduceByKey(_+_)

gendersNo.collect().foreach(x=> println(f"Genero : ${x._1} porcentaje:"+(x._2.toFloat/nosufrieronAtaque.count())*100 ))


val gendersSi = sufrieronAtaque.map(row => row.split(",")(1) -> 1).reduceByKey(_+_)

gendersSi.collect().foreach(x=> println(f"Genero : ${x._1} porcentaje:"+(x._2.toFloat/nosufrieronAtaque.count())*100 ))



// SPARK SQL

val data = spark.read.option("delimiter", ",").option("header", "true").csv("data/stroke_clean.csv")
data.createOrReplaceTempView("data")

val total = spark.sql("SELECT count(1) as sufrieronAtaque FROM data WHERE stroke == 1")
val numtotal = total.first.getLong(0)

val genderCuenta = spark.sql("SELECT gender , stroke , count(*) as gcount FROM data GROUP BY gender , stroke ORDER BY stroke" )
genderCuenta.show

val totals = spark.sql("SELECT stroke , count(*) as count FROM data GROUP BY stroke ORDER BY stroke" )
totals.show

genderCuenta.createOrReplaceTempView("genderCuenta")
totals.createOrReplaceTempView("totals")

val percentages = spark.sql("SELECT  gender , genderCuenta.stroke, 100*gcount/ count  as porcentaje  FROM genderCuenta INNER JOIN totals  on genderCuenta.stroke = totals.stroke " )
percentages.show
