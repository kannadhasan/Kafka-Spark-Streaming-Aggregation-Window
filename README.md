#Kafka-Spark-Streaming-Aggregation-Window

**Kafka Producer**

java -cp /home/kannadhasan/workspace/spark-streamming/target/spark-streamming-0.0.1-SNAPOT-jar-with-dependencies.jar com.spark.kafka.kafkaProducer localhost:9092 demo 1000

{"key1":8,"key2":6,"key5":15,"key3":4,"key4":16,"ts":"2017/08/14 10:39:21"} {"key1":9,"key2":2,"key5":11,"key3":5,"key4":14,"ts":"2017/08/14 10:39:22"} {"key1":7,"key2":5,"key5":11,"key3":6,"key4":13,"ts":"2017/08/14 10:39:23"} {"key1":9,"key2":6,"key5":17,"key3":4,"key4":15,"ts":"2017/08/14 10:39:24"} {"key1":10,"key2":4,"key5":11,"key3":5,"key4":15,"ts":"2017/08/14 10:39:26"} {"key1":10,"key2":2,"key5":15,"key3":3,"key4":16,"ts":"2017/08/14 10:39:27"} {"key1":5,"key2":2,"key5":17,"key3":2,"key4":15,"ts":"2017/08/14 10:39:28"}

**Aggrigated results**

2 Minutes Aggrigation window Completed ...Waiting for next batch
{"key1_min":5,"key1_max":10,"key1_sum":154.0,"key1_cout":20,"key2_min":2,"key2_max":6,"key2_sum":85.0,"key2_cout":20,"key5_min":11,"key5_max":17,"key5_sum":277.0,"key5_cout":20,"key3_min":1,"key3_max":7,"key3_sum":83.0,"key3_cout":20,"key4_min":12,"key4_max":16,"key4_sum":281.0,"key4_cout":20,"ts":"List(2017/08/14 10:53:59, 2017/08/14 10:53:58, 2017/08/14 10:53:57, 2017/08/14 10:53:56, 2017/08/14 10:53:55, 2017/08/14 10:53:54, 2017/08/14 10:53:52, 2017/08/14 10:53:51, 2017/08/14 10:53:50, 2017/08/14 10:53:49, 2017/08/14 10:53:48, 2017/08/14 10:53:47, 2017/08/14 10:53:46, 2017/08/14 10:53:45, 2017/08/14 10:53:44, 2017/08/14 10:53:42, 2017/08/14 10:53:41, 2017/08/14 10:53:40, 2017/08/14 10:53:39, 2017/08/14 10:53:38)"}


**Spark submit**

Spark Submit ./bin/spark-submit --class com.spark.stream.KafkaSparkAggrigation --master spark://kannadhasan:7077 --deploy-mode cluster /home/kannadhasan/workspace/spark-streamming/target/spark-streamming-0.0.1-SNAPSHOT-jar-with-dependencies.jar localhost:9092 demo 2

