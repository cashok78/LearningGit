package streaming.join

import java.sql.Timestamp
import java.util.UUID

import entity.RateData
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

object streamingFileJoin extends App {

  def getOrderSchema(): StructType={
    StructType(
        StructField("orderId", StringType),
        StructField("orderSymbol", StringType),
        StructField("price", Double),
        StructField("quantity", Long),
        StructField("orderTime", TimestampType))
  }

  def getQuoteSchema(): StructType={
      new StructType().add("quoteSymbol", "string").add("bid", "double").add("ask", "double").add("quantity", "long").add("quoteTime", "timestamp")
  }


  def readFileStream(fileSrc:String, schema:StructType):sql.DataFrame{
    val fileStreamDf = sparkSession.readStream
    .option("header", "true")
    .schema(schema)
    .csv(fileSrc)
  }

  def main(args: Array[String]): Unit = {
    val chkPt = "C://temp//" + UUID.randomUUID.toString
    val spark: SparkSession = SparkSession.builder()
      .appName("streamStreamInnerJoin")
      .master("local[*]")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "1")

    val logger = LogManager.getRootLogger
    logger.setLevel(Level.ERROR)

    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 3)
      .option("numPartitions", 1)
      .option("rampUpTime", 1)
      .option("includeTimestamp", value = true)
      .load()

    import spark.implicits._

    val tickers = Seq("0011.HK","0000.HK","0055.HK","2111.HK","2323.HK","1167.HK","0700.HK")
    val udfFn = udf{value:Int => tickers(value %7)}
    val rateData = df.as[RateData]
    var quoteDs = rateData.where("value % 10 < 5")
      .withColumn("quoteType",  concat(lit("quoteType "),rateData.col("value")))
      //.withColumn("lastName",  concat(lit("lastName"),rateData.col("value")))
      //.withColumn("e_deptId", lit(floor(rateData.col("value")/10)))
      .withColumn("quoteSym", udfFn('value))
      .withColumnRenamed("value", "quotePrice")
      .withColumnRenamed("timestamp", "quoteTime")
      .withWatermark("quoteTime","5 seconds")

    var orderDs = rateData.where("value % 10 >= 5")
      .withColumn("orderId", concat(lit("id"),floor(rateData.col("value")/10)))
      // .withColumn("d_deptId", lit(floor(rateData.col("value")/10)))
      .withColumnRenamed("timestamp","orderTime")
      .withColumn("orderSym", udfFn('value))
      .withColumnRenamed("value", "orderPrice")
      .withWatermark("orderTime","5 seconds")

    val joinedDS =  orderDs.join(quoteDs,expr("""orderSym = quoteSym and orderTime >= quoteTime and orderTime <= quoteTime + interval 10 seconds"""), joinType = "leftOuter")
    //,'lastName, 'e_deptId,'d_deptId)
    val joinedStream = joinedDS.select('orderId,'orderSym,'quotePrice,'orderPrice,'orderTime, 'quoteTime)
      .groupBy("orderId", "orderSym")
      .agg(last("quoteTime").as("qteTime"))
      .select('orderId,'orderSym,'qteTime) //'quotePrice,'orderPrice,'orderTime,
      .writeStream
      .format("console")
      .option("truncate", false)
      .outputMode("Update")
      .option("checkpointLocation", chkPt)
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    //   .queryName("joinedTable")
    spark.streams.awaitAnyTermination()
  }

}