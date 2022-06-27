//2 ¿A qué edad es más probable sufrir un ataque?

sc.setJobGroup("Consulta 2", "Spark Scala")
val cicloVida = (edad: Float)=>{
    if(edad <11)
       "Infante"
    else if(edad >=11 && edad <=18)
        "Adolescente"
    else if(edad >=19 && edad <27 )
        "Joven"
    else if(edad >=27 && edad <=59)
        "Adulto"
    else
        "Adulto Mayor"
}
val perConAtaque =  mp.filter(row => row.split(",")(11)=="1")
val ataquesPorCicloVida = perConAtaque.map(row => cicloVida(row.split(",")(2).toFloat)->1).reduceByKey(_+_)
ataquesPorCicloVida.collect().foreach(x=> println(f"Ciclo de vida: ${x._1} pacientes: ${x._2}"))

sc.setJobGroup("Consulta 2", "Spark Scala-showPorcentaje")
ataquesPorCicloVida.collect().foreach(x=> println(f"Ciclo de vida: ${x._1} porcentaje:"+(x._2.toFloat/perConAtaque.count())*100))

//SQL
sc.setJobGroup("Consulta 2", "Spark SQL")
val infantes = spark.sql("SELECT count(1) as pacientes FROM dfTableDemo INNER JOIN dfTableSalud ON dfTableDemo.id = dfTableSalud.id WHERE Age < 11 AND stroke == 1").withColumn("ciclo",lit("infantes"))
val adolescentes = spark.sql("SELECT count(1) as pacientes FROM dfTableDemo INNER JOIN dfTableSalud ON dfTableDemo.id = dfTableSalud.id WHERE Age >= 11 AND Age <=18 AND stroke == 1").withColumn("ciclo",lit("adolescentes"))
val jovenes = spark.sql("SELECT count(1) as pacientes FROM dfTableDemo INNER JOIN dfTableSalud ON dfTableDemo.id = dfTableSalud.id WHERE Age >= 19 AND Age <27 AND stroke == 1").withColumn("ciclo",lit("jovenes"))
val adultos = spark.sql("SELECT count(1) as pacientes FROM dfTableDemo INNER JOIN dfTableSalud ON dfTableDemo.id = dfTableSalud.id WHERE Age >= 27 AND Age <60 AND stroke == 1").withColumn("ciclo",lit("adultos"))
val adultos_mayor = spark.sql("SELECT count(1) as pacientes FROM dfTableDemo INNER JOIN dfTableSalud ON dfTableDemo.id = dfTableSalud.id WHERE Age >=60 AND stroke == 1").withColumn("ciclo",lit("adultos mayor"))
infantes.union(adolescentes).union(jovenes).union(adultos).union(adultos_mayor).show

