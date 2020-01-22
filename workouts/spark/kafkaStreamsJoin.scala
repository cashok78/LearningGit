package streamingJoins
import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection

import org.apache.spark.sql.streaming.Trigger

object kafkaStreamsJoin {
  val bootstrapServers = "localhost:9092"
  val subscribeType = "subscribe"
  def main(args: Array[String]): Unit = {

    val orderTopic = "DATA.INPUT1"
    val quoteTopic = "DATA.INPUT2"
    val orderSchema = ScalaReflection.schemaFor[Order].dataType.asInstanceOf[StructType]
    val quoteSchema = ScalaReflection.schemaFor[Quote].dataType.asInstanceOf[StructType]

    val checkpointLocation = "C://temp//" + UUID.randomUUID.toString
    val spark = SparkSession.builder.config("spark.master", "local").appName("kafkaStreamsJoin").getOrCreate()

    // Create DataSet representing the stream of input lines from kafka
    val orderStream = readKafkaStream(spark, orderTopic, orderSchema)
    val quoteStream = readKafkaStream(spark, quoteTopic, quoteSchema)

    val watermarkTime = "60 seconds"
    val triggerTime = 5000

    //import spark.implicits._
    val orderStreamJson = orderStream.select("jsonString.*")
      .withColumn("orderTime", from_unixtime(col("orderTime")).cast("timestamp"))
      .withWatermark("orderTime", watermarkTime)
      //.writeStream.trigger(Trigger.ProcessingTime(triggerTime)).format("console").start()

    val quoteStreamJson = quoteStream.select("jsonString.*")
      .withColumn("quoteTime", from_unixtime(col("quoteTime")).cast("timestamp"))
        .withColumnRenamed("symbol","qsym")
        .withColumnRenamed("size", "qsize")
      .withWatermark("quoteTime", watermarkTime)
      // .writeStream.trigger(Trigger.ProcessingTime(triggerTime)).format("console").start()

    val joinedStream = orderStreamJson.join(quoteStreamJson,expr("""symbol = qsym AND orderTime >= quoteTime and orderTime <= quoteTime + interval 60 seconds"""), joinType = "rightOuter")
    // val joinedStream = orderStreamJson.join(quoteStreamJson,expr("""symbol = qsym AND orderTime >= quoteTime AND quoteTime <= orderTime + interval 60 seconds"""), joinType = "rightOuter")
    joinedStream.printSchema()

    val result = joinedStream.writeStream.queryName("joinedStream").trigger(Trigger.ProcessingTime(triggerTime)).format("console").option("checkpointLocation", checkpointLocation).start()
    //joinedStream.createOrReplaceTempView("updates")
    //spark.sql("select * from joinedStream").show()

    result.awaitTermination()
  }

  def readKafkaStream(spark:SparkSession, kafkaTopic:String, schema:StructType):DataFrame ={
    val jsonOptions = {}
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option(subscribeType, kafkaTopic)
      .option("startingOffsets", "latest")
      .load()
      .select(from_json(col("value").cast("string"), schema).alias("jsonString"))
  }

  //{"orderTime":1579656206,"symbol":0011.HK, "size":10}
  case class Order(orderTime:Long,symbol:String, size:Long)

  //{"quoteTime":1579656206,"symbol":0011.HK, "size":1000, "bid":12, "ask":13}
  case class Quote(quoteTime:Long, symbol:String, size:Long, bid:Double, ask:Double)
}
