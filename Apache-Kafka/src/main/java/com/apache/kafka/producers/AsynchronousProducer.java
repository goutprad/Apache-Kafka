package com.apache.kafka.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AsynchronousProducer {

	public static void main(String[] args) {
		String topicName = "AsynchronousProducerTopic";
		String key = "key1";
		String value = "value1";

		Properties prop = new Properties();
		prop.put("bootstrap.servers", "localhost:9092,localhost:9093");
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(prop);
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);

		producer.send(record, new MyProducerCallBack());
		System.out.println("AsynchronousProducer call completed");
		producer.close();
	}
}

class MyProducerCallBack implements Callback {

	@Override
	public void onCompletion(RecordMetadata recordMetaData, Exception e) {
		if (e != null) {
			System.out.println("AsynchronousProducer failed with an exception");
		} else {
			System.out.println("AsynchronousProducer call Success:");
		}
	}

}
