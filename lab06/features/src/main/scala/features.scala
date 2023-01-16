import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.text.SimpleDateFormat
import java.util.TimeZone
import org.apache.spark.ml.feature.VectorAssembler

object features {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession =
      SparkSession
        .builder
        .master(master = "local")
        .appName(name = "plomt")
        .getOrCreate()

    import spark.implicits._

    val toDay = udf { (value: Long) => {
      val currentDay: SimpleDateFormat = new SimpleDateFormat("EEE")
      currentDay.setTimeZone(TimeZone.getTimeZone("UTC"))
      currentDay.format(value)
    } }

    val toHour = udf { (value: Long) => {
      val currentHour: SimpleDateFormat = new SimpleDateFormat("HH")
      currentHour.setTimeZone(TimeZone.getTimeZone("UTC"))
      currentHour.format(value)
    } }

    val toPart = udf { (value: String) => {
      val valueLong = value.toLong
      if (valueLong >= 9 && valueLong < 18)
        "job"
      else if (valueLong >= 18 && valueLong <= 23)
        "evening"
      else
        "night"
    } }

    val corDomain = udf { (value: String) => {
      value.replace(".", "/")
    } }

    val parqDf = spark
      .read
      .parquet("/user/pavel.lomtev/users-items/20200429")

    val jsonDf = spark
      .read
      .json("hdfs:///labs/laba03/weblogs.json")
      .select($"uid",explode($"visits").alias("time_domain")).select("uid", "time_domain.url", "time_domain.timestamp")
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .na.drop(Seq("domain"))

    val topDomainsDf = jsonDf.groupBy("domain").count.orderBy(col("count").desc).limit(1000).orderBy(col("domain"))
    val topDomainsList = topDomainsDf.select("domain").collect.map(_(0)).toList

    val filterJsonDf = jsonDf.filter(col("domain").isin(topDomainsList:_*))

    val upFilterJsonDf = filterJsonDf
      .withColumn("day", toDay(col("timestamp")))
      .withColumn("hour", toHour(col("timestamp")))
      .withColumn("partDay", toPart(col("hour")))
      .withColumn("corDomain", corDomain(col("domain")))

    val groupedJson = upFilterJsonDf
      .groupBy("uid", "corDomain")
      .count

    // Create a sorted array of categories
    val categories = groupedJson
      .select(col("corDomain"))
      .distinct.map(_.getString(0))
      .collect
      .sorted

    // Prepare vector assemble
    val assembler =  new VectorAssembler()
      .setInputCols(categories)
      .setOutputCol("domain_features")

    // Aggregation expressions
    val exprs = categories.map(
      c => sum(when($"corDomain" === c, $"count").otherwise(lit(0))).alias(c))

    val transformed = assembler.transform(
      groupedJson.groupBy($"uid").agg(exprs.head, exprs.tail: _*))
      .select($"uid", $"domain_features")

    val dayCounts = upFilterJsonDf
      .groupBy("uid")
      .agg(
        sum(when($"day" === "Mon", 1).otherwise(0)).alias("web_day_mon"),
        sum(when($"day" === "Tue", 1).otherwise(0)).alias("web_day_tue"),
        sum(when($"day" === "Wed", 1).otherwise(0)).alias("web_day_wed"),
        sum(when($"day" === "Thu", 1).otherwise(0)).alias("web_day_thu"),
        sum(when($"day" === "Fri", 1).otherwise(0)).alias("web_day_fri"),
        sum(when($"day" === "Sat", 1).otherwise(0)).alias("web_day_sat"),
        sum(when($"day" === "Sun", 1).otherwise(0)).alias("web_day_sun"),

        sum(when($"hour" === "00", 1).otherwise(0)).alias("web_hour_0"),
        sum(when($"hour" === "01", 1).otherwise(0)).alias("web_hour_1"),
        sum(when($"hour" === "02", 1).otherwise(0)).alias("web_hour_2"),
        sum(when($"hour" === "03", 1).otherwise(0)).alias("web_hour_3"),
        sum(when($"hour" === "04", 1).otherwise(0)).alias("web_hour_4"),
        sum(when($"hour" === "05", 1).otherwise(0)).alias("web_hour_5"),
        sum(when($"hour" === "06", 1).otherwise(0)).alias("web_hour_6"),
        sum(when($"hour" === "07", 1).otherwise(0)).alias("web_hour_7"),
        sum(when($"hour" === "08", 1).otherwise(0)).alias("web_hour_8"),
        sum(when($"hour" === "09", 1).otherwise(0)).alias("web_hour_9"),
        sum(when($"hour" === "10", 1).otherwise(0)).alias("web_hour_10"),
        sum(when($"hour" === "11", 1).otherwise(0)).alias("web_hour_11"),
        sum(when($"hour" === "12", 1).otherwise(0)).alias("web_hour_12"),
        sum(when($"hour" === "13", 1).otherwise(0)).alias("web_hour_13"),
        sum(when($"hour" === "14", 1).otherwise(0)).alias("web_hour_14"),
        sum(when($"hour" === "15", 1).otherwise(0)).alias("web_hour_15"),
        sum(when($"hour" === "16", 1).otherwise(0)).alias("web_hour_16"),
        sum(when($"hour" === "17", 1).otherwise(0)).alias("web_hour_17"),
        sum(when($"hour" === "18", 1).otherwise(0)).alias("web_hour_18"),
        sum(when($"hour" === "19", 1).otherwise(0)).alias("web_hour_19"),
        sum(when($"hour" === "20", 1).otherwise(0)).alias("web_hour_20"),
        sum(when($"hour" === "21", 1).otherwise(0)).alias("web_hour_21"),
        sum(when($"hour" === "22", 1).otherwise(0)).alias("web_hour_22"),
        sum(when($"hour" === "23", 1).otherwise(0)).alias("web_hour_23"),

        (sum(when($"partDay" === "job", 1).otherwise(0)) / count($"partDay")).alias("web_fraction_work_hours"),
        (sum(when($"partDay" === "evening", 1).otherwise(0)) / count($"partDay")).alias("web_fraction_evening_hours")
      )
      .withColumnRenamed("uid", "uid_d")

    val df = dayCounts.join(transformed, dayCounts("uid_d") === transformed("uid"), "inner")
      .drop("uid_d")
      .withColumnRenamed("uid", "uid_d")

    val res = parqDf.join(df, parqDf("uid") === df("uid_d"), "full")
      .drop("uid_d")

    res.write.parquet("/user/pavel.lomtev/features")
  }
}