
#Kafka-Spark-Streaming-Aggregation-Window

**Kafka Producer **

java -cp /home/kannadhasan/workspace/spark-streamming/target/spark-streamming-0.0.1-SNAPOT-jar-with-dependencies.jar com.spark.kafka.kafkaProducer localhost:9092 demo 1000

{"key1":8,"key2":6,"key5":15,"key3":4,"key4":16,"ts":"2017/08/14 10:39:21"} {"key1":9,"key2":2,"key5":11,"key3":5,"key4":14,"ts":"2017/08/14 10:39:22"} {"key1":7,"key2":5,"key5":11,"key3":6,"key4":13,"ts":"2017/08/14 10:39:23"} {"key1":9,"key2":6,"key5":17,"key3":4,"key4":15,"ts":"2017/08/14 10:39:24"} {"key1":10,"key2":4,"key5":11,"key3":5,"key4":15,"ts":"2017/08/14 10:39:26"} {"key1":10,"key2":2,"key5":15,"key3":3,"key4":16,"ts":"2017/08/14 10:39:27"} {"key1":5,"key2":2,"key5":17,"key3":2,"key4":15,"ts":"2017/08/14 10:39:28"}

**Spark submit**

Spark Submit ./bin/spark-submit --class com.spark.stream.KafkaSparkAggrigation --master spark://kannadhasan:7077 --deploy-mode cluster /home/kannadhasan/workspace/spark-streamming/target/spark-streamming-0.0.1-SNAPSHOT-jar-with-dependencies.jar localhost:9092 demo 2
