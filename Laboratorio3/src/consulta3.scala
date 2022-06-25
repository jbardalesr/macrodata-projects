val data = sc.textFile("data-limpia.csv")
val mp = data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter}
val zones_stroke = mp.map(s =>  s.split(",")(7)+'-'+s.split(",")(11)).map(res_type => (res_type, 1)).reduceByKey(_ + _)
zones_stroke.saveAsTextFile("consulta3.txt")