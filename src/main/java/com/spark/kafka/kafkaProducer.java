package com.spark.kafka;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

public class kafkaProducer {

	public static void main(String[] args) throws InterruptedException {
		String ServerAddress = args[0];
		String topic = args[1];
		int freq = Integer.parseInt(args[2]);
		if (args.length == 3) {
			while (true) {
				JSONObject data = new JSONObject();
				data.put("key1", getRandomNumberInRange(5, 10));
				data.put("key2", getRandomNumberInRange(2, 6));
				data.put("key3", getRandomNumberInRange(1, 7));
				data.put("key4", getRandomNumberInRange(12, 16));
				data.put("key5", getRandomNumberInRange(11, 17));
				
				long millis = System.currentTimeMillis();
				String timeStamp = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss", Locale.ENGLISH).format(new Date(millis));
				data.put("ts", timeStamp);

				Properties props = new Properties();

				props.put("bootstrap.servers", ServerAddress);
				props.put("acks", "all");
				props.put("retries", 0);
				props.put("batch.size", 16384);
				props.put("linger.ms", 1);
				props.put("buffer.memory", 33554432);

				props.put("key.serializer", StringSerializer.class.getName());
				props.put("value.serializer", StringSerializer.class.getName());

				Producer<String, String> producer = new KafkaProducer<String, String>(props);

				producer.send(new ProducerRecord<String, String>(topic, data.toString()));
				System.out.println(data);
				producer.close();
				Thread.sleep(freq);
			}

		}
		else{System.out.println("localhost:9092 demo 10000");}
	}

	private static int getRandomNumberInRange(int min, int max) {

		if (min >= max) {
			throw new IllegalArgumentException("max must be greater than min");
		}

		Random r = new Random();
		return r.nextInt((max - min) + 1) + min;
	}
}