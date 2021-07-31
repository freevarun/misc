package com.kafka.tutorial.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	public static void main(String[] args) {
		//System.out.println("Hello World...!");
		
		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
		
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-forth-application";
		//String groupId = "my-fifth-test-latest-application";
		//String groupId = "my-fifth-test-latest2-application";
		String topic = "first_topic";
		
		//create consumer config
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		//earliest - read from the beginning 
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		//
		//latest - read from new offset
		//properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		
		//this is used to disable auto commit of offset
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
		//this will control the number of records poll at a time
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"20");
		
		//create the consumer
		KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);
		
		//subscribe the consumer to the topic(s)
		//
		//way to subscribe one topic
		//consumer.subscribe(Collections.singleton(topic));
		
		//
		//way to subscribe multiple topics
		//consumer.subscribe(Arrays.asList("first-topic","new-topic"));
		consumer.subscribe(Arrays.asList(topic));
		
		//poll new data
		while(true){
			//
			//it is deplicated do using Duration class as per API suggestion
			//consumer.poll(100);
			ConsumerRecords<String,String> records 
						= consumer.poll(Duration.ofMillis(100));//new in Kafka 2.0.0
			
			
			for(ConsumerRecord<String, String> record : records){
				logger.info("Received "+records.count()+" records");
				//2 strategies
				//1. Kafka generic Id
				String id = record.topic()+"_"+record.partition()+"_"+record.offset();
				
				//this is the id which needed to be identified that will help in understanding to make consumer idempotent
				//we need to make in such a way that if consumer group goes down we will received the same message again 
				//that to atleast once mode
				System.out.println(id);
				logger.info("Key: "+record.key()+", Value: "+record.value());
				logger.info("Partitions: "+record.partition()+", Offset: "+record.offset());
				try {
					Thread.sleep(10);// introduce the delay in processing the record
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			//logger.info("Commiting offset...");
			consumer.commitSync();
			//logger.info("Offset has been commited...");
		}
		
		
	}

}
