import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.PipelineModel

object test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession =
      SparkSession
        .builder
        .master(master = "local")
        .appName(name = "plomt")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5, org.apache.kafka:kafka-clients:0.10.1.0")
        .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> "pavel_lomtev",
      "startingOffsets" -> "earliest",
      "maxOffsetPerTrigger" -> "5"
    )

    val structSchema = StructType(
      List(
        StructField("timestamp", LongType, nullable = true),
        StructField("url", StringType, nullable = true)
      )
    )

    val dataSchema = StructType(
      List(
        StructField("uid", StringType, nullable = true),
        StructField("visits", ArrayType(structSchema), nullable = true)
      )
    )

    val sdf = spark.readStream.format("kafka").options(kafkaParams).load
    val parsedSdf = sdf.select(from_json($"value".cast(StringType), dataSchema).as("value"))
    val test = parsedSdf
      .select($"value.uid".alias("uid"), explode($"value.visits").alias("time_domain"))
      .select("uid", "time_domain.url")
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .drop("host", "url")

    val model_path = "/user/pavel.lomtev/model"
    val model = PipelineModel.load(model_path)

    model.transform(test)
      .select(to_json(struct(col("uid"), col("gender_age"))).as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("topic", "pavel_lomtev_lab07_out")
      .option("checkpointLocation", "/user/pavel.lomtev/chk/lab7")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()
  }
}