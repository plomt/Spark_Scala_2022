import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object users_items {

  val normalize_buy: UserDefinedFunction = udf { (value: String) => {
    val res_buy = ("buy_" + value.toLowerCase).replace("[", "")
      .replace("]", "")
      .replace(" ", "_")
      .replace("-", "_")
    res_buy
  }}

  val normalize_view: UserDefinedFunction = udf { (value: String) => {
    val res_view = ("view_" + value.toLowerCase).replace("[", "")
      .replace("]", "")
      .replace(" ", "_")
      .replace("-", "_")
    res_view
  }}

  def readHdfsView(path: String, spark: SparkSession): DataFrame = {
    val basePath="hdfs://spark-master-1.newprolab.com:8020" + path

    val viewDf = spark
      .read
      .option("basePath", basePath)
      .json(basePath + "/view/p_date=*/*")
      .na.drop(Seq("uid"))
      .withColumn("parsed_items", normalize_view(col("item_id")))
      .drop("item_id")
      .withColumnRenamed("uid", "uid_view")
    viewDf
  }

  def readHdfsBuy(path: String, spark: SparkSession): DataFrame = {
    val basePath="hdfs://spark-master-1.newprolab.com:8020" + path

    val buyDf = spark
      .read
      .option("basePath", basePath)
      .json(basePath + "/buy/p_date=*/*")
      .na.drop(Seq("uid"))
      .withColumn("parsed_items", normalize_buy(col("item_id")))
      .drop("item_id")
      .withColumnRenamed("uid", "uid_buy")
    buyDf
  }

  def readHdfsView2(path: String, spark: SparkSession): DataFrame = {
    val basePath = path

    val viewDf = spark
      .read
      .option("basePath", basePath)
      .format("json")
      .json(basePath + "/view/p_date=*/*.json")
      .na.drop(Seq("uid"))
      .withColumn("parsed_items", normalize_view(col("item_id")))
      .drop("item_id")
      .withColumnRenamed("uid", "uid_view")
    viewDf
  }

  def readHdfsBuy2(path: String, spark: SparkSession): DataFrame = {
    val basePath = path + "/buy"

    val buyDf = spark
      .read
      .option("basePath", basePath)
      .format("json")
      .json(basePath + "/p_date=*/*.json")
      .na.drop(Seq("uid"))
      .withColumn("parsed_items", normalize_buy(col("item_id")))
      .drop("item_id")
      .withColumnRenamed("uid", "uid_buy")
    buyDf
  }

  def maxDate(viewDf: DataFrame, buyDf: DataFrame): Long = {
    val maxNumsDf = viewDf.select(max(col("date").cast("long"))).union(buyDf.select(max(col("date").cast("long")))).withColumnRenamed("max(CAST(date AS BIGINT))", "max_nums")
    val max_num = maxNumsDf.select(max(col("max_nums"))).first()(0).asInstanceOf[Number].longValue
    max_num
  }

  def createPivot(viewDf: DataFrame, buyDf: DataFrame, uids: DataFrame): DataFrame = {
    val pivotDFView = viewDf.groupBy("uid_view").pivot("parsed_items").count.na.fill(0)
    val pivotDFBuy = buyDf.groupBy("uid_buy").pivot("parsed_items").count.na.fill(0)

    val resPivotDf = uids
      .join(pivotDFBuy, uids("uids") === pivotDFBuy("uid_buy"), "left")
      .join(pivotDFView, uids("uids") === pivotDFView("uid_view"), "left")
      .drop("uid_buy", "uid_view")
      .na.fill(0)
    resPivotDf
  }

  def merge2Df(df1: DataFrame, df2: DataFrame): DataFrame = {
    val new_columns1 = df1.columns.toSet diff df2.columns.toSet
    val upDf2 = new_columns1.foldLeft(df2)((df2, month) => df2.withColumn(month, lit(0)))

    val new_columns2 = upDf2.columns.toSet diff df1.columns.toSet
    val upDf1 = new_columns2.foldLeft(df1)((df1, month) => df1.withColumn(month, lit(0)))

    val unionDf = upDf1.union(upDf2.select(upDf1.columns.toSeq.map(c => col(c)):_*))
    val groupDf = unionDf.groupBy("uid").sum()
    val newColumns = groupDf.columns.map(x => x.replace("sum(", "").replace(")", ""))
    val resultDf = groupDf.toDF(newColumns:_*)
    resultDf
  }

  def saveDf(date: Long, df: DataFrame, path: String): Unit = {
    val dateStr = date.toString
    df
      .write
      .mode("overwrite")
      .parquet(path + "/" + dateStr)
  }


  def main(args: Array[String]): Unit = {
    val spark: SparkSession =
      SparkSession
        .builder
        .master(master = "local")
        .appName(name = "plomt")
        .getOrCreate()

    val update = spark.conf.get("spark.users_items.update")
    val output_dir = spark.conf.get("spark.users_items.output_dir")
    val input_dir = spark.conf.get("spark.users_items.input_dir")

    import spark.implicits._

    if (update.toInt == 0) {
      val viewDf = readHdfsView(input_dir, spark)
      val buyDf = readHdfsBuy(input_dir, spark)

      val max_date = maxDate(viewDf, buyDf) - 1

      val uids = viewDf.select('uid_view).union(buyDf.select('uid_buy)).distinct.withColumnRenamed("uid_view", "uids")

      val pivotDf = createPivot(viewDf, buyDf, uids).withColumnRenamed("uids", "uid")

      saveDf(max_date, pivotDf, output_dir)
    }
    else {
      val viewDf = readHdfsView2(input_dir, spark)
      val buyDf = readHdfsBuy2(input_dir, spark)

      val max_date = maxDate(viewDf, buyDf) - 1

      val uids = viewDf.select('uid_view).union(buyDf.select('uid_buy)).distinct.withColumnRenamed("uid_view", "uids")

      val pivotDf = createPivot(viewDf, buyDf, uids).withColumnRenamed("uids", "uid")

      val existsDf = spark.read.parquet(output_dir + "/20200429")

      val resultDf = merge2Df(existsDf, pivotDf)

      saveDf(20200430, resultDf, output_dir)
    }
  }
}
