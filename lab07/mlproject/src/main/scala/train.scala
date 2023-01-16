import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{callUDF, collect_list, explode, lit, lower, regexp_replace}

object train {
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

    val training = spark
      .read
      .json("hdfs:///labs/laba07/laba07.json")
      .select($"gender_age", $"uid", explode($"visits").alias("time_domain")).select("gender_age", "uid", "time_domain.url")
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .drop("host", "url")
      .groupBy("uid", "gender_age")
      .agg(collect_list("domain"))
      .withColumnRenamed("collect_list(domain)", "domains")

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")
      .fit(training)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val ind2str = new IndexToString()
      .setInputCol(lr.getPredictionCol)
      .setOutputCol("gender_age")
      .setLabels(indexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr, ind2str))

    val model = pipeline.fit(training)

    val model_path = "/user/pavel.lomtev/model"

    model.write.overwrite().save(model_path)

  }
}