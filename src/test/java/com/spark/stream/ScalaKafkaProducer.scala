package com.spark.stream


import kafka.producer.ProducerConfig
import java.util.Properties
import kafka.producer.Producer
import scala.util.Random
import kafka.producer.Producer
import kafka.producer.Producer
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import java.util.Date
import com.google.gson.JsonObject

object ScalaKafkaProducer extends App {
  val events = args(0).toInt
  val topic = args(1)
  val brokers = args(2)
  val rnd = new Random()
  val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  //props.put("partitioner.class", "com.colobu.kafka.SimplePartitioner")
  props.put("producer.type", "async")
  //props.put("request.required.acks", "1")

  val config = new ProducerConfig(props)
  
  val producer = new Producer[String, String](config)
  while(true){
  val t = System.currentTimeMillis()
  
  for (nEvents <- Range(0, events)) {
    val runtime = new Date().getTime();
    val ip = "192.168.2." + rnd.nextInt(255);
    val msg = runtime + "," + nEvents + ",www.example.com," + ip;
    
    
    
 
    
    val data = new KeyedMessage[String, String](topic, ip, msg);
    producer.send(data);
    println("data Send")
  }
  Thread.sleep(1000)
  
  System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t));
  }
  producer.close();
}