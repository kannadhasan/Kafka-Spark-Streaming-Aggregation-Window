package com.spark.stream

import org.mongodb.scala._
import com.mongodb.async.client.MongoClientSettings
import com.mongodb.connection.ClusterSettings
import scala.collection.JavaConverters._

object ConnectionPool{
  
   val clusterSettings: ClusterSettings = ClusterSettings.builder().hosts(List(new ServerAddress("localhost")).asJava).build()
  val settings: MongoClientSettings = MongoClientSettings.builder().codecRegistry(org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY).clusterSettings(clusterSettings).build()
  val mongoClient: MongoClient = MongoClient(settings)
  val database: MongoDatabase = mongoClient.getDatabase("test")
  val collection: MongoCollection[Document] = database.getCollection("spark_streamming")
  println("connected")

println("-----------------------*********************************************************-")
  def getConnection : MongoCollection[Document] ={
    collection
  }
}