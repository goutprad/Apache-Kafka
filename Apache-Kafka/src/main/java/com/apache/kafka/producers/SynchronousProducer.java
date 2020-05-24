package com.apache.kafka.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SynchronousProducer {

	public static void main(String[] args) {
		String topicName = "SynchronousProducerTopic";
		String key = "key1";
		String value = "value1";

		Properties prop = new Properties();
		prop.put("bootstrap.servers", "localhost:9092,localhost:9093");
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(prop);
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);

		try {
			RecordMetadata metaData = producer.send(record).get();
			System.out.println(
					"Message is sent to Partition no " + metaData.partition() + " and offset " + metaData.offset());
			System.out.println("SynchronousProducer Completed with success.");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("SynchronousProducer failed with an exception");
		} finally {
			producer.close();
		}
	}

}
