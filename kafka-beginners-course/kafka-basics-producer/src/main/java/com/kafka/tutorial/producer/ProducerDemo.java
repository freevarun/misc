package com.kafka.tutorial.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {
		String bootstrapServer = "127.0.0.1:9092";
		
		//create producer property
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create producer
		KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);
		
		//create the record
		ProducerRecord<String,String> record = new ProducerRecord<String,String>("first_topic","Hello world!");
		
		//send data - asynchronous
		producer.send(record);
		
		// flush data
		producer.flush();
		//close producer
		producer.close();
	}

}
