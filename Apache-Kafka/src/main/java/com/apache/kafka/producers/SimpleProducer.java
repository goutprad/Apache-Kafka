package com.apache.kafka.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer {

	public static void main(String[] args) {
		String topicName = "SimpleProducerTopic";
		String key = "Key1";
		String value = "value-1";
		Properties prop = new Properties();
		prop.put("bootstrap.servers", "localhost:9092,localhost:9093");
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(prop);
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);

		producer.send(record);
		producer.close();
		System.out.println("Successfully Sent!");
	}

}
