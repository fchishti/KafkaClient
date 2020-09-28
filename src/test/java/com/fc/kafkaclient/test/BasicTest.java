package com.fc.kafkaclient.test;

import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import com.fc.kafkaclient.KafkaClientConfig;
import com.fc.kafkaclient.consumer.KafkaConsumer;
import com.fc.kafkaclient.producer.KafkaProducer;

public class BasicTest {
	
	public static final String TOPIC = KafkaClientConfig.loadConfig().getProperty("kafka.topic");
	public static final String DATAKEY = "TestKey";
	public static final String DATAVALUE = "This is TestData";
	
	@Test
	public void clientTest() throws Exception {

		Map<String, String> returnedData = null;
		
		try (KafkaProducer<String, String> producer = KafkaProducer.build(TOPIC, new StringSerializer(),
				new StringSerializer())) {
			producer.sendMessage(TOPIC, true, DATAKEY, DATAVALUE);
		}
		
		try (KafkaConsumer<String, String> consumer = KafkaConsumer.build(TOPIC, new StringDeserializer(),
				new StringDeserializer())) {
			for(int i = 0; i < 5; i++) {
				returnedData = consumer.readAsMap();
				if(!returnedData.isEmpty())
					break;
				TimeUnit.SECONDS.sleep(2);
			}
		}
		
		assertTrue(!returnedData.isEmpty());
	}
}
