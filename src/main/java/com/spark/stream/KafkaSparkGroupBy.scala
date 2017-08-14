package com.spark.stream

import org.apache.spark.SparkConf

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.mongodb.scala.Completed
import org.mongodb.scala.Document
import org.mongodb.scala.Observer
import kafka.serializer.StringDecoder
import java.io.Serializable
import com.google.gson.JsonObject
import com.google.gson.JsonParser
//import org.codehaus.jackson.JsonParser

object KafkaSparkGroupBy {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println(s"""
						|Usage: KafkaSparkGroupBy <brokers> <topics>
						|  <brokers> is a list of one or more Kafka brokers
						|  <topics> is a list of one or more kafka topics to consume from
						|
						""".stripMargin)

      System.exit(1)
    }

    val Array(brokers, topics) = args
    println(brokers)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaPOC").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "group.id" -> "SparkStream_test")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val lines = messages.map(_._2)

    val res = lines.flatMap { x => x.split("\n") }

    val result = res.foreachRDD(rdd =>
      rdd.foreachPartition {
        println("Started New Batch")
        var rawList: List[JsonObject] = List()
        partitionOfRecords =>
          partitionOfRecords.foreach { x =>
            var jsonData: JsonObject = new JsonParser().parse(x).getAsJsonObject
            //  println(jsonData)
            rawList = jsonData :: rawList
          }
          //Process
          // println(rawList.size)

          //Groupby
          //rawList.groupBy(_.get("ts")).foreach{x => println(x)}

          //single groupby with sorted
          /*var map : Map[String, List[JsonObject]] = Map()
						rawList.groupBy(_.get("ts")).foreach{x => 
						map += (x._1.toString() -> x._2)
				}
				
				map.toSeq.sortBy(_._1).foreach{data =>
				  println(data)
				  }*/

          //Multiple Groupby with sorted
          var map: Map[String, List[JsonObject]] = Map()
          rawList.groupBy(x => (x.get("ts"), x.get("deviceType"))).foreach { x =>
            map += (x._1.toString() -> x._2)
          }
          map.toSeq.sortBy(_._1).foreach { data =>
            println(data)
          }

      })

    ssc.start()
    ssc.awaitTermination()

  }

}
