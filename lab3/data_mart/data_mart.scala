import org.apache.spark.sql.{SparkSession, Column}
import org.apache.spark.sql.functions._

object data_mart {
  def main(args: Array[String]) = {
    val spark: SparkSession =
      SparkSession
        .builder
        .master(master = "local[*]")
        .appName(name = "plomt")
        .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    // -----------------------------------------------------------------------------------------------------------------
    spark.conf.set("spark.cassandra.connection.host", "10.0.0.31")
    spark.conf.set("spark.cassandra.connection.port", "9042")
    spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
    spark.conf.set("spark.cassandra.input.consistency.level", "ONE")
    val tableOptsCassandra = Map("table" -> "clients", "keyspace" -> "labdata")

    val casDf = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(tableOptsCassandra)
      .load()

    val age = casDf.select("age").collect.toList.map(x => x.toString.replace("[", "").replace("]", "").toInt).map(x => {
      if (x >= 18 && x <= 24)
        "18-24"
      else if (x >= 25 && x <= 34)
        "25-34"
      else if (x >= 35 && x <= 44)
        "35-44"
      else if (x >= 45 && x <= 54)
        "45-54"
      else
        ">=55"
    })

    val uid = casDf.select("uid").collect.toList.map(x => x.toString.replace("[", "").replace("]", ""))
    val zippedUidAge = (uid, age).zipped.toList
    val uidAgeCatDF = sc.parallelize(zippedUidAge).toDF("uid_", "age_cat")

    val resCasDf = casDf
      .join(uidAgeCatDF, casDf("uid") === uidAgeCatDF("uid_"), "inner")
      .drop("uid_")
      .drop("age")

    // -----------------------------------------------------------------------------------------------------------------
    spark.conf.set("es.nodes", "10.0.0.31")
    spark.conf.set("es.port", "9200")
    spark.conf.set("es.net.http.auth.user","pavel.lomtev")
    spark.conf.set("es.net.http.auth.pass", "Ef9gyFRg")
    spark.conf.set("es.resource", "vistis")

    val elasticDf = spark
      .read
      .format("org.elasticsearch.spark.sql")
      .load("visits")

    val elasticDfCleaned = elasticDf.na.drop(Seq("uid"))
    val shopColumns = elasticDfCleaned
      .select(lower('category))
      .map(x => ("shop_" + x.toString).replace("[", "").replace("]", "").replace(" ", "_").replace("-", "_"))
      .collect
      .toList

    val lowerColumnsShop = elasticDfCleaned
      .select(lower('category))
      .map(x => x.toString.replace("[", "").replace("]", ""))
      .collect
      .toList

    val zippedColumnsShop = (shopColumns, lowerColumnsShop).zipped.toList
    val columnsDfShop = sc.parallelize(zippedColumnsShop).toDF("shops", "lower").distinct

    val catCounterDf = elasticDfCleaned
      .select(col("*"), lower('category).alias("lower_category"))
      .groupBy("uid", "lower_category")
      .count

    val catCounterDfShop = catCounterDf
      .join(columnsDfShop, catCounterDf("lower_category") === columnsDfShop("lower"), "inner")
      .drop("lower")
      .drop("lower_category")
      .withColumnRenamed("uid", "uid_shop")

    val pivotDFShop = catCounterDfShop.groupBy("uid_shop").pivot("shops").sum("count").na.fill(0)

    // -----------------------------------------------------------------------------------------------------------------
    val jsonDf = spark
      .read
      .json("hdfs:///labs/laba03/weblogs.json")

    val jsonUrlDomainDf = jsonDf.select(col("uid"), explode(col("visits")).alias("time_domain")).select("uid", "time_domain").select("uid", "time_domain.url")
    val pattern = "(?:(?:https?)(?::?)(?:%[0-9A-F]{2})*(?:\\/\\/)?)?(?:www\\.)?([A-Za-zА-Яа-я0-9._+-]+)(?:\\/?.*)"

    val cleanJsonUrlDomainDf = jsonUrlDomainDf
      .withColumn("domain", regexp_extract(jsonUrlDomainDf("url"), pattern, 1))
      .drop("url")
      .withColumnRenamed("domain", "domain_c")

    // -----------------------------------------------------------------------------------------------------------------
    val postgresDf = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
      .option("dbtable", "domain_cats")
      .option("user", "pavel_lomtev")
      .option("password", "Ef9gyFRg")
      .option("driver", "org.postgresql.Driver")
      .load()

    val webColumns = postgresDf
                        .select(lower('category))
                        .map(x => ("web_" + x.toString).replace("[", "").replace("]", "").replace(" ", "_").replace("-", "_"))
                        .collect
                        .toList

    val lowerColumnsWeb = postgresDf
      .select(lower('category))
      .map(x => x.toString.replace("[", "").replace("]", ""))
      .collect
      .toList

    val zippedColumnsWeb = (webColumns, lowerColumnsWeb).zipped.toList
    val columnsDfWeb = sc.parallelize(zippedColumnsWeb).toDF("web", "lower").distinct

    val res_web = cleanJsonUrlDomainDf
      .join(postgresDf, postgresDf("domain") === cleanJsonUrlDomainDf("domain_c"), "inner")
      .drop("domain_c")
      .groupBy("uid", "category")
      .count
      .orderBy("uid")

    val catCounterDfWeb = res_web
      .join(columnsDfWeb, res_web("category") === columnsDfWeb("lower"), "inner")
      .drop("category")
      .drop("lower")
      .withColumnRenamed("uid", "uid_web")

    val pivotDFWeb = catCounterDfWeb.groupBy("uid_web").pivot("web").sum("count").na.fill(0)

    // -----------------------------------------------------------------------------------------------------------------
    val resDf = resCasDf.join(pivotDFWeb, resCasDf("uid") === pivotDFWeb("uid_web"), "leftouter")
      .join(pivotDFShop, resCasDf("uid") === pivotDFShop("uid_shop"), "leftouter")
      .drop("uid_web", "uid_shop")
      .na.fill(0)

    resDf.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/pavel_lomtev")
      .option("dbtable", "clients")
      .option("user", "pavel_lomtev")
      .option("password", "Ef9gyFRg")
      .option("driver", "org.postgresql.Driver")
      .save()
  }
}
