import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, count, udf}

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.text.DecimalFormat

object agg {
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

    val toDate = udf { (value: String) => {
      LocalDateTime.parse(value, DateTimeFormatter.ofPattern("yyyy-dd-MM HH:mm:ss"))
        .atZone(ZoneOffset.UTC)
        .toInstant
        .toEpochMilli
    }
    }

    val toFormat = udf { (value: Long) => {
      val decimalFormat: DecimalFormat = new DecimalFormat("0.#")
      decimalFormat.format(value)
    }
    }

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> "lab04_input_data",
      "startingOffsets" -> "earliest",
      "maxOffsetPerTrigger" -> "5"
    )

    val dataSchema = StructType(
      List(
        StructField("event_type", StringType, nullable = true),
        StructField("category", StringType, nullable = true),
        StructField("item_id", StringType, nullable = true),
        StructField("item_price", LongType, nullable = true),
        StructField("uid", StringType, nullable = true),
        StructField("timestamp", StringType, nullable = true)
      )
    )

    val sdf = spark.readStream.format("kafka").options(kafkaParams).load
    val parsedSdf = sdf.select(from_json($"value".cast(StringType), dataSchema).as("value"))
    val parsed = parsedSdf.select($"value.*").withColumn("timestampt", to_timestamp(col("timestamp").cast("long") / 1000))

    val windowedCounts = parsed
      .withWatermark("timestampt", "60 minutes")
      .groupBy(window($"timestampt", "60 minutes"))
      .agg(
        count(when($"uid".isNotNull,1).otherwise(0)).alias("visitors"),
        sum(when($"event_type" === "view",0).otherwise($"item_price")).alias("revenue"),
        sum(when($"event_type" === "view",0).otherwise(1)).alias("purchases"),
        ((sum(when($"event_type" === "view",0).otherwise($"item_price"))) / (sum(when($"event_type" === "view",0).otherwise(1)))).as("aov")
      )

    val res = windowedCounts
      .withColumn("start_ts", toFormat(toDate(date_format(col("window.start"), "yyyy-dd-MM HH:mm:ss")) / 1000))
      .withColumn("end_ts", toFormat(toDate(date_format(col("window.end"), "yyyy-dd-MM HH:mm:ss")) / 1000))
      .drop("window")

    res.select(to_json(struct(col("start_ts"), col("end_ts"), col("revenue"), col("visitors"), col("purchases"), col("aov"))))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", "pavel_lomtev_lab04b_out")
      .option("checkpointLocation", "/user/pavel.lomtev/chk/lab4b")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
  }
}