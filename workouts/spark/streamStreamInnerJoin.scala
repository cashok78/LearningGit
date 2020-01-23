package streaming.join

//concept derived out of https://github.com/sylvesterdj/LearningScala/tree/master/SparkStreamingPOC/src/main/scala/streaming/join
import java.util.UUID

import entity.RateData
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object streamStreamInnerJoin extends App {

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
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .option("rampUpTime", 1)
    .option("includeTimestamp", value = true)
    .load()

  import spark.implicits._

  val tickers = Seq("0011.HK","0000.HK","0055.HK","2111.HK","2323.HK","1167.HK","0700.HK")
  val udfFn = udf{value:Int => tickers(value %7)}
  val rateData = df.as[RateData]
  val employeeDS = rateData.where("value % 10 != 0")
    .withColumn("firstName",  concat(lit("firstName"),rateData.col("value")))
    //.withColumn("lastName",  concat(lit("lastName"),rateData.col("value")))
    //.withColumn("e_deptId", lit(floor(rateData.col("value")/10)))
    .withColumn("symbol", udfFn('value))
    .withColumnRenamed("value", "e_id")
    .withColumnRenamed("timestamp", "empTime")

  val departmentDS = rateData.where("value % 10 == 0")
    .withColumn("dept", concat(lit("dept"),floor(rateData.col("value")/10)))
   // .withColumn("d_deptId", lit(floor(rateData.col("value")/10)))
    .withColumnRenamed("timestamp","deptTime")
    .withColumn("symbol", udfFn('value))
    .withColumnRenamed("value", "d_id")


  val joinedDS =  departmentDS.join(employeeDS,"symbol")

  val joinedStream = joinedDS.select('symbol,'e_id,'d_id,'empTime, 'deptTime,'firstName) //,'lastName, 'e_deptId,'d_deptId)
    .writeStream
    .format("console")
    .option("truncate", false)
    .option("checkpointLocation", chkPt)
    .trigger(Trigger.ProcessingTime("2 seconds"))
    .start()

  //   .queryName("joinedTable")
  spark.streams.awaitAnyTermination()
}