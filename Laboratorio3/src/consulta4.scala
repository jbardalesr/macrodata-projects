// ¿Edades promedio de los que sufrieron ataque según el tipo de empleo?
val data = sc.textFile("data-numerica.csv")
val mp = data.mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else iter}
val stroke_data = mp.filter(x => x.split(",")(11).equals("1"))
val type_stroke = stroke_data.map(s =>  s.split(",")(6)+'-'+s.split(",")(2))
val average_age = type_stroke.map(type_age => (type_age.split("-")(0), type_age.split("-")(1).toFloat)).reduceByKey(_ + _)
zones_stroke.saveAsTextFile("output4")