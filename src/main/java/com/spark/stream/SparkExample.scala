package com.spark.stream
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

import org.bson.BSONObject

import com.mongodb.hadoop.{
  MongoInputFormat, MongoOutputFormat,
  BSONFileInputFormat, BSONFileOutputFormat}
import com.mongodb.hadoop.io.MongoUpdateWritable
import org.bson.BasicBSONObject;

object SparkExample extends App {
  // Set up the configuration for reading from MongoDB.
  val mongoConfig = new Configuration()
  // MongoInputFormat allows us to read from a live MongoDB instance.
  // We could also use BSONFileInputFormat to read BSON snapshots.
  // MongoDB connection string naming a collection to read.
  // If using BSON, use "mapred.input.dir" to configure the directory
  // where the BSON files are located instead.
  mongoConfig.set("mongo.input.uri",
    "mongodb://localhost:27017/test.orders")

  val sparkConf = new SparkConf()
  val sc = new SparkContext("local", "SparkExample", sparkConf)

  // Create an RDD backed by the MongoDB collection.
  val documents = sc.newAPIHadoopRDD(
    mongoConfig,                // Configuration
    classOf[MongoInputFormat],  // InputFormat
    classOf[Object],            // Key type
    classOf[BSONObject])        // Value type
println(documents.count())
  // Create a separate Configuration for saving data back to MongoDB.
  val outputConfig = new Configuration()
  outputConfig.set("mongo.output.uri",
    "mongodb://localhost:27017/test.kanna")

    
    
    
  // Save this RDD as a Hadoop "file".
  // The path argument is unused; all documents will go to "mongo.output.uri".
 documents.saveAsNewAPIHadoopFile(
    "file:///this-is-completely-unused",
    classOf[Object],
    classOf[BSONObject],
    classOf[MongoOutputFormat[Object, BSONObject]],
    outputConfig)

  // We can also save this back to a BSON file.
 /*val bsonOutputConfig = new Configuration()
  documents.saveAsNewAPIHadoopFile(
    "hdfs://localhost:8020/user/spark/bson-demo",
    classOf[Object],
    classOf[BSONObject],
    classOf[BSONFileOutputFormat[Object, BSONObject]])*/

  // We can choose to update documents in an existing collection by using the
  // MongoUpdateWritable class instead of BSONObject. First, we have to create
  // the update operations we want to perform by mapping them across our current
  // RDD.
  val updates = documents.mapValues(
    value => new MongoUpdateWritable(
      new BasicBSONObject("_id", value.get("_id")),  // Query
      new BasicBSONObject("$set", new BasicBSONObject("foo", "bar")),  // Update operation
      false,  // Upsert
      false   // Update multiple documents
    )
  )

  
  // Now we call saveAsNewAPIHadoopFile, using MongoUpdateWritable as the
  // value class.
  updates.saveAsNewAPIHadoopFile(
    "file:///this-is-completely-unused",
    classOf[Object],
    classOf[MongoUpdateWritable],
    classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
    outputConfig)
    print("Done...")
}
