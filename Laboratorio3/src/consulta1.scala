sc.setJobGroup("Consulta 1", "Spark Scala")

//1 ¿Cómo afecta el hecho de estar o haber estado casado a tener un ataque?
//Mostrar distribución general por estado civil y ataque
mp.map(row => (row.split(",")(11)+"-"+row.split(",")(5),1)).reduceByKey(_+_).
collect().foreach(x=> println(f"ataque?: ${x._1.split("-")(0)} - casado?:${x._1.split("-")(1)}  pacientes:"+x._2))

sc.setJobGroup("Consulta 1", "Spark Scala - análisis complementario")
val perCasadas = mp.filter(row => row.split(",")(5)=="Yes")
val perCasadasConAtaque = perCasadas.map(row => (row.split(",")(11)+"-"+row.split(",")(5),1)).reduceByKey(_+_)
//Mostrar resultado
perCasadasConAtaque.collect().foreach(x=> println(f"ataque?: ${x._1.split("-")(0)} - casado?:${x._1.split("-")(1)}  pacientes:"+x._2))
//Mostrar resultado por porcentajes
perCasadasConAtaque.collect().foreach(x=> println(f"ataque?: ${x._1.split("-")(0)} - casado?:${x._1.split("-")(1)} porcentaje:"+(x._2.toFloat/perCasadas.count())*100))

sc.setJobGroup("Consulta 1", "Spark Scala - análisis complementario")
val perNoCasadas = mp.filter(row => row.split(",")(5)=="No")
val perNoCasadasConAtaque = perNoCasadas.map(row => (row.split(",")(11)+"-"+row.split(",")(5),1)).reduceByKey(_+_)
//Mostrar resultado
perNoCasadasConAtaque.collect().foreach(x=> println(f"ataque?: ${x._1.split("-")(0)} - casado?:${x._1.split("-")(1)}  pacientes:"+x._2))
//Mostrar resultado por porcentajes
perNoCasadasConAtaque.collect().foreach(x=> println(f"ataque?: ${x._1.split("-")(0)} - casado?:${x._1.split("-")(1)} porcentaje:"+(x._2.toFloat/perNoCasadas.count())*100))


//SQL 
sc.setJobGroup("Consulta 1", "Spark SQL")
spark.sql("SELECT ever_married, stroke, count(1) as pacientes FROM dfTableDemo INNER JOIN dfTableSalud ON dfTableDemo.id = dfTableSalud.id GROUP BY stroke,ever_married").show
