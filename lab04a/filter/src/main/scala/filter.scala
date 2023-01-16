import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.Dataset
import java.text.SimpleDateFormat

object filter {
  def main(args: Array[String]) = {

    val spark: SparkSession =
      SparkSession
        .builder
        .master(master = "local")
        .appName(name = "plomt")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5, org.apache.kafka:kafka-clients:0.10.1.0")
        .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val offset = spark.conf.get("spark.filter.offset")
    val topic = spark.conf.get("spark.filter.topic_name")

    val start_offset = offset match {
      case "earliest" => "earliest"
      case "latest"   => "latest"
      case _          => f"""{"$topic": { "0": $offset }}"""
    }

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> topic,
      "startingOffsets" ->  start_offset
    )

    val df = spark
      .read
      .format("kafka")
      .options(kafkaParams)
      .load

    val jsonString = df.select('value.cast("string")).as[String]
    val parsed = spark.read.json(jsonString)

    val toDate = udf { (value: Long) => {
      val df: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
      df.format(value)
    } }

    val parsedTime = parsed.withColumn("date", toDate('timestamp.cast("long")))

    val buyDf = parsedTime.filter('event_type === "buy").withColumn("p_date", col("date"))
    val viewDf = parsedTime.filter('event_type === "view").withColumn("p_date", col("date"))

    val path = spark.conf.get("spark.filter.output_dir_prefix")

    def saveDf[T](name: String, df: Dataset[T]): Unit = {
      df
        .write
        .format("json")
        .mode("overwrite")
        .partitionBy("p_date")
        .json(path + "/" + name)
    }

    saveDf("buy", buyDf)
    saveDf("view", viewDf)
  }
}
