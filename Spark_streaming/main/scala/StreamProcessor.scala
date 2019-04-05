

import java.time.{LocalDate, Period}


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.collection.JavaConverters._

object StreamsProcessor {
  def main(args: Array[String]): Unit = {
    new StreamsProcessor("localhost:9092").process()
  }
}

class StreamsProcessor(brokers: String) {

  def process(): Unit = {

    val spark = SparkSession.builder()
      .appName("kafka-tutorials")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", "test4")
      .load()


    val personJsonDf: DataFrame = inputDf.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("temp", DataTypes.FloatType)
      .add("ph", DataTypes.FloatType)
      .add("glucose", DataTypes.FloatType)
      .add("cholesterol", DataTypes.FloatType)
      .add("heartbeat", DataTypes.FloatType)
      .add("battery", DataTypes.StringType)

    val personNestedDf = personJsonDf.select(from_json($"value", struct).as("person"))

    val personFlattenedDf = personNestedDf.selectExpr("person.temp", "person.ph", "person.glucose", "person.cholesterol","person.heartbeat", "person.battery")

    val alertDf = personFlattenedDf.filter(personFlattenedDf("battery") < 15)

    val resDf = alertDf.select(
      concat($"temp", $"glucose", $"cholesterol",$"heartbeat").as("key"),
      alertDf.col("battery").cast {
        DataTypes.StringType
      }.as("value"))

    val kafkaOutput = resDf.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", "alert")
      .option("checkpointLocation", "/home/baskar/Documents/kafka_2.12-2.2.0/checkpointalert")
      .start()

    kafkaOutput.awaitTermination()

    //val jsonOutput = personFlattenedDf.writeStream
      //.format("json")
      //.option("checkpointLocation", "/home/baskar/Documents/kafka_2.12-2.2.0/jsondata")
      //.option("path", "/home/baskar/Documents/kafka_2.12-2.2.0/checkpoint")
      //.start()
    //jsonOutput.awaitTermination()




    spark.streams.awaitAnyTermination()


  }

}

