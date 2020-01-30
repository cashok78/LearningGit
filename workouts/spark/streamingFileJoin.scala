package streaming.join

import java.util
import java.util.UUID

import entity.{Order, Quote}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.examples.sql.streaming.{SessionInfo, SessionUpdate}
import org.apache.spark.sql
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object streamingFileJoin extends App {

  def readFileStream(spark:SparkSession, fileSrc:String, schema:StructType):sql.DataFrame = {
     spark.readStream.option("header", "true").schema(schema).option("maxFilesPerTrigger", 1).csv(fileSrc)
  }

  override def main(args: Array[String]): Unit = {

    val chkPt = "C://temp//" + UUID.randomUUID.toString
    val spark: SparkSession = SparkSession.builder()
      .appName("streamingFileJoin")
      .master("local[*]")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "1")

    val logger = LogManager.getRootLogger
    logger.setLevel(Level.ERROR)
    val orderSchema = Encoders.product[Order].schema
    val quoteSchema = Encoders.product[Quote].schema

    orderSchema.printTreeString()
    quoteSchema.printTreeString()
    import spark.implicits._

    val quoteStream = readFileStream(spark, "E:\\Java\\HugeDatasets\\csv\\quotes", quoteSchema).withWatermark("quoteTime", "2 seconds")
    val orderStream = readFileStream(spark, "E:\\Java\\HugeDatasets\\csv\\orders", orderSchema).withWatermark("orderTime", "2 seconds")
    quoteStream.as[Quote].groupByKey(_.quoteSymbol).flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout())(flatMapForQuotes)

    /*
    orderStream.join(quoteStream,expr("""quoteSymbol = orderSymbol and quoteTime <= orderTime and orderTime <= quoteTime + interval 2 seconds"""), joinType =  "leftOuter")
        .groupBy("orderId")
        .agg(last("orderSymbol").as("symbol"), last("quoteTime").as("quoteTime"), last("ask").as("ask"),last("bid").as("bid"))
        .writeStream
        .trigger(Trigger.ProcessingTime("2 seconds"))
        .format("console")
        .outputMode("update")
        .start()
*/
       //orderStream.writeStream      .format("console")      .outputMode("update")      .start()
    spark.streams.awaitAnyTermination()



  }

  def flatMapForQuotes(symbol:String, quotes:Iterator[Quote],state:GroupState[Quote]): Iterator[Quote]={
    var quotesForSym = ArrayBuffer[Quote]();

    if(quotes != null && quotes.hasNext) {
      val finalQuote = quotes.maxBy(_.quoteTime)
      state.update(finalQuote)
      quotes.foreach(qte => quotesForSym+=qte)
    }
    else
      if(state.get != null)
        sys.error(s" $symbol is neither present in State nor available in current batch")
      else
        quotesForSym += state.get

      quotesForSym.toIterator
    }
}