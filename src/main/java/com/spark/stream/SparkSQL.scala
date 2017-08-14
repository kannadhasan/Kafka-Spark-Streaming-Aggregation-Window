package com.spark.stream

import org.apache.spark._
import org.apache.spark.sql._

object SparkSQL {
 def main(args: Array[String]): Unit = {
   println("Starting here.....")
    val conf = new SparkConf().setAppName("signal-aggregation").setMaster("local")
    val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)
    
    val stringRDD = sc.parallelize(Seq(""" 
  { "isActive": false,
    "balance": "$1,431.73",
    "picture": "http://placehold.it/32x32",
    "age": 35,
    "eyeColor": "red"
  }""",
   """{
    "isActive": true,
    "balance": "$2,515.60",
    "picture": "http://placehold.it/32x32",
    "age": 34,
    "eyeColor": "blue"
  }""", 
  """{
    "isActive": false,
    "balance": "$3,765.29",
    "picture": "http://placehold.it/32x32",
    "age": 26,
    "eyeColor": "blue",
    "eyelor": "blue"
  }""")
)

sqlContext.jsonRDD(stringRDD).toDF().registerTempTable("testjson")



val Result=sqlContext.sql("SELECT sum(age) from testjson").collect
Result.foreach { x => println("Hi",x) }
   
 val Result1=sqlContext.sql("SELECT age,balance from testjson").collect
Result1.foreach { x => println(x) }
 
  val Result2=sqlContext.sql("SELECT min(age),max(age),avg(age) from testjson").collect
Result2.foreach { x => println(x) }
  
   
  val Result3=sqlContext.sql("SELECT min(age),max(age),avg(age) from testjson WHERE age=26 GROUP BY eyeColor").collect
  
Result3.foreach { x => println(x) }

  }
}