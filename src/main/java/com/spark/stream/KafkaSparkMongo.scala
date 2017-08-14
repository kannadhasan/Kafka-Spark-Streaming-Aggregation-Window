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

object KafkaSparkMongo {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println(s"""|Usage: kafkaSparkMongo <brokers> <topics>""".stripMargin)

      System.exit(1)
    }
 
    val Array(brokers, topics) = args
    println(brokers)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaPOC").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,"group.id" -> "SparkStream_test")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val lines = messages.map(_._2)
    val res = lines.flatMap { x => x.split("\n") }
    var count: Int = 0;
    res.foreachRDD(rdd =>
      rdd.foreachPartition { partitionOfRecords =>
    
        var connection = ConnectionPool.getConnection
        partitionOfRecords.foreach { x =>
          println(x + "\n---------")
          val doc: Document = Document(x)
          connection.insertOne(doc).subscribe(new Observer[Completed] {
            override def onNext(result: Completed): Unit = println(s"onNext: $result")
            override def onError(e: Throwable): Unit = println(s"onError: $e")
            override def onComplete(): Unit = {
              count = count + 1
             println("Completed")
            }
          })

        }
        connection= null
     
      })

    ssc.start()
    ssc.awaitTermination()

  }

}
