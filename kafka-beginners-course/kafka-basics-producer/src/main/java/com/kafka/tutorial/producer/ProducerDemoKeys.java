package com.kafka.tutorial.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {
	static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		String bootstrapServer = "127.0.0.1:9092";
		
		//create producer property
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create producer
		KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);
		
		for(int i = 0; i < 10; i++){
			
			String topic = "first_topic";
			String value = "Hello world without get...! "+Integer.toString(i);
			String key = "id_"+Integer.toString(i);
			
			//create the record
			ProducerRecord<String,String> record = new ProducerRecord<String,String>(
							topic,key,value);
			
			logger.info("Keys: "+key);//log the Key
			
			//send data - asynchronous
			producer.send(record,new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					//Execute every time record is successfully send or exception is thrown
					if(e == null){
						//record was send successfully
						logger.info("Received new metaData. \n"+
									"Topic: "+recordMetadata.topic()+"\n"+
									"Partition: "+recordMetadata.partition()+"\n"+
									"Offset: "+recordMetadata.offset() +"\n"+
									"TimeStamp: "+recordMetadata.timestamp());
					}else{
						logger.error("Error while producing: "+e);
					}
					
				}
			});
			//.get();//block the .send() to make it synchronized don't do this in production
		}
		
		// flush data
		producer.flush();
		//close producer
		producer.close();
	}

}
