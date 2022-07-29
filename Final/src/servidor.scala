import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ 
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{Pipeline, PipelineModel}

sc.setLogLevel("ERROR")
val ssc = new StreamingContext(sc, Seconds(1))
val registros1 = ssc.socketTextStream("192.168.1.13",9999)
val registros2 = ssc.socketTextStream("192.168.1.13",9998)
val registros = registros1.union(registros2)

val saveModel = PipelineModel.load("./tmp/rf-model")
registros.foreachRDD(rdd => {
    if(!rdd.isEmpty){
        var regDF = rdd.toDF("features")
        var regDF2 = regDF.select(split(col("features"),",").getItem(0).cast("String").as("id"),
                    split(col("features"),",").getItem(1).cast("Int").as("gender"),
                    split(col("features"),",").getItem(2).cast("Int").as("age"),
                    split(col("features"),",").getItem(3).cast("Int").as("age_range"),
                    split(col("features"),",").getItem(4).cast("Int").as("hypertension"),
                    split(col("features"),",").getItem(5).cast("Int").as("heart_disease"),
                    split(col("features"),",").getItem(6).cast("Int").as("ever_married"),
                    split(col("features"),",").getItem(7).cast("Int").as("work_type"),
                    split(col("features"),",").getItem(8).cast("Int").as("Residence_type"),
                    split(col("features"),",").getItem(9).cast("Double").as("avg_glucose_level"),
                    split(col("features"),",").getItem(10).cast("Double").as("bmi"),
                    split(col("features"),",").getItem(11).cast("Int").as("smoking_status"))
                    .drop("features")

        regDF2.coalesce(1)
        .write.option("header","true")
        .option("sep",",")
        .mode("append")
        .csv("./tmp/registros")

        val predictions = saveModel.transform(regDF2)
        print(predictions)
        predictions
        .select("id", "prediction")
        .show()

        predictions.select("id", "prediction").coalesce(1)
        .write.option("header","true")
        .option("sep",",")
        .mode("append")
        .csv("./tmp/predicciones")
    }
})

ssc.start()    
ssc.awaitTermination()