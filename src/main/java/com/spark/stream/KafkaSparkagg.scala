package com.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.mongodb.scala.Document
import org.mongodb.scala.Observer

import kafka.serializer.StringDecoder
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import org.apache.spark.streaming.Minutes

object KafkaSparkAggrigation {
  var min: Int = _
  var max: Int = _
  var resJson: JsonObject = new JsonObject
  var sum: Double = 0
  var noOfKeys: Int = 0
  var tsList: List[String] = List()
  var count: Int = 0;

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      println("Provide: kafkaSparkMongo <brokers> <topics> <minute>")

      System.exit(1)
    }

    val Array(brokers, topics, windowsInterval) = args
    println(brokers)
    var windows: Int = windowsInterval.toInt
    val sparkConf = new SparkConf().setAppName("KafkaSpark").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Minutes(windows))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "group.id" -> "SparkStream_test")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2)
    val res = lines.flatMap { x => x.split("\n") }

    res.foreachRDD(rdd =>

      rdd.foreachPartition {
        count = 1;
        resJson = new JsonObject
        tsList = List[String]()
        //    println("Consuming Next " + windows + "secound Batch")
        partitionOfRecords =>
          partitionOfRecords.foreach { x =>

            // println(x + "\n---------")
            var jsonparser = new JsonParser
            var jsom: JsonObject = jsonparser.parse(x).getAsJsonObject
            var iter = jsom.entrySet().iterator()
            var key: String = null;
            while (iter.hasNext()) {
              key = iter.next().getKey
              if (!key.equalsIgnoreCase("ts")) {
                var value: Int = jsom.get(key).getAsInt
                if (count > 1) {
                  if (resJson.has(key + "_min") && resJson.get(key + "_min").getAsDouble > value) {
                    min = value
                    resJson.addProperty(key + "_min", min)

                  }
                  if (resJson.has(key + "_max") && resJson.get(key + "_max").getAsDouble < value) {
                    max = value
                    resJson.addProperty(key + "_max", max)
                  }
                  if (resJson.has(key + "_sum"))
                    sum = resJson.get(key + "_sum").getAsDouble + value

                } else {
                  min = value
                  max = value
                  sum = value
                  resJson.addProperty(key + "_min", min)
                  resJson.addProperty(key + "_max", max)
                }

                resJson.addProperty(key + "_sum", sum)
                resJson.addProperty(key + "_cout", count)

              } else {
                var value: String = jsom.get(key).getAsString
                tsList ::= value
                resJson.addProperty("ts", tsList.toString())
                count = count + 1

              }

            }

          }

          println(windowsInterval+ " Minutes Aggrigation window Completed ...Waiting for next batch")
          println(resJson)
      })

    ssc.start()
    ssc.awaitTermination()

  }

}
