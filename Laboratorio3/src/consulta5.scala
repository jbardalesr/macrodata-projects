//5 ¿Cual es la clasificacion del bmi de las personas que sufrieron un ataque?


val load_data = sc.textFile("data/stroke_clean.csv")
val data = load_data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

sc.setJobGroup("Consulta 5", "Rangos BMI")

val clasificacion = (BMI: Float)=>{
    if  (BMI == null)
        "nulo"
    else if(BMI <= 18.5)
       "Debajo"
    else if(18.5 < BMI && BMI <=25.0)
        "Normal"
    else if(25.0  < BMI && BMI <=30.0 )
        "Sobrepeso"
    else
        "Obesidad"
}

val sufrieronAtaque =  data.filter(row => row.split(",")(11)=="1")
val ataquesBMI = sufrieronAtaque.map(row => clasificacion(row.split(",")(9).toFloat)->1).reduceByKey(_+_)
ataquesBMI.collect().foreach(x=> println(f"Clasificacion BMI : ${x._1} porcentaje:"+(x._2.toFloat /sufrieronAtaque.count())*100))


val nosufrieronAtaque =  data.filter(row => row.split(",")(11)=="0")
val noAtaquesBMI = nosufrieronAtaque.map(row => clasificacion(row.split(",")(9).toFloat)->1).reduceByKey(_+_)
noAtaquesBMI.collect().foreach(x=> println(f"Clasificacion BMI : ${x._1} porcentaje:"+(x._2.toFloat/nosufrieronAtaque.count())*100))



// SPARK SQL

val data = spark.read.option("delimiter", ",").option("header", "true").csv("data/stroke_clean.csv")

data.createOrReplaceTempView("data")

val total = spark.sql("SELECT count(1) as sufrieronAtaque FROM data WHERE stroke == 1")

val numtotal = total.first.getLong(0)

val debajo = spark.sql("SELECT count(1) as sufrieronAtaque FROM data WHERE  bmi <= 18.5  AND stroke == 1").withColumn("clasif",lit("debajo"))
val normal = spark.sql("SELECT count(1) as sufrieronAtaque FROM data WHERE bmi  > 18.5 AND bmi <= 25.0 AND stroke == 1").withColumn("clasif",lit("normal"))
val sobrepeso = spark.sql("SELECT count(1) as sufrieronAtaque FROM data WHERE  bmi > 25.0 AND  bmi <= 30.0 AND stroke == 1").withColumn("clasif",lit("sobrepeso"))
val obesidad = spark.sql("SELECT count(1) as sufrieronAtaque FROM data WHERE bmi > 30.0 AND stroke == 1").withColumn("clasif",lit("obesidad"))

val cuenta  = debajo.union(normal).union(sobrepeso).union(obesidad).withColumn("total",lit(numtotal))

cuenta.show
cuenta.createOrReplaceTempView("cuenta")

spark.sql("SELECT 100 * cuenta.sufrieronAtaque / cuenta.total  as porcentaje , cuenta.clasif FROM cuenta").show
