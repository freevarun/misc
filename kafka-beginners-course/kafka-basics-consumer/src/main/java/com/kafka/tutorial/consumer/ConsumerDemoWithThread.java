package com.kafka.tutorial.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {

	public static void main(String[] args) {
		new ConsumerDemoWithThread().run();
	}
	
	private ConsumerDemoWithThread(){
	}
	
	private void run(){
		
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
		
		String bootstrapServers = "127.0.0.1:9092";
		//String groupId = "my-forth-application";
		//String groupId = "my-fifth-test-latest-application";
		//String groupId = "my-fifth-test-latest2-application";
		String groupId = "my-sixth-application";
		String topic = "first_topic";
		
		//latch for dealing with multiple threads
		CountDownLatch latch = new CountDownLatch(1);
		
		logger.info("Create the consumer thread!");
		
		//create the consumer runnable
		Runnable myConsumerThreadRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);
		
		//start the thread
		Thread myThread = new Thread(myConsumerThreadRunnable);
		myThread.start();
		
		//add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Caught shutdown hook...!");
			((ConsumerRunnable) myConsumerThreadRunnable).shutdown();
			try {
				latch.await();
			} catch (Exception e) {
				e.printStackTrace();
			}
			logger.info("Application has exited...!");
		}));
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application is interrupted "+ e);
		}finally{
			logger.info("Application is closing...!");
		}
	}
	
	public class ConsumerRunnable implements Runnable {
		
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
		
		private CountDownLatch latch;
		
		//create the consumer
		private KafkaConsumer<String,String> consumer;
		
		public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
			
			//create consumer config
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			//earliest - read from the beginning  - possible values are earliest/latest/none
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			this.latch = latch;
			
			//create the consumer
			consumer = new KafkaConsumer<String,String>(properties);
			
			//subscribe the consumer to the topic(s)
			//
			//way to subscribe multiple topics
			consumer.subscribe(Arrays.asList(topic));
		}
		
		@Override
		public void run() {
			try{
				//poll new data
				while(true){
					//
					//it is deplicated so using Duration class as per API suggestion
					//consumer.poll(100);
					ConsumerRecords<String,String> records 
								= consumer.poll(Duration.ofMillis(100));//new in Kafka 2.0.0
					
					for(ConsumerRecord<String, String> record : records){
						logger.info("Key: "+record.key()+", Value: "+record.value());
						logger.info("Partitions: "+record.partition()+", Offset: "+record.offset());
					}
				}
			}catch(WakeupException e){
				logger.info("Received shutdown signal!");
			}finally {
				consumer.close();
				//this is tell the main code we're done with the consumer
				latch.countDown();
			}
		}
		
		public void shutdown(){
			
			//the wakeup() method is special method to interrupt consumer.poll()
			//it will throw the exception WakeUpException
			consumer.wakeup();
		}
	}
}
