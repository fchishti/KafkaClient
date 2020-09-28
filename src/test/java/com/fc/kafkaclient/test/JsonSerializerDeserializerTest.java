package com.fc.kafkaclient.test;

import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import com.fc.kafkaclient.KafkaClientConfig;
import com.fc.kafkaclient.consumer.KafkaConsumer;
import com.fc.kafkaclient.json.KafkaJsonDeserializer;
import com.fc.kafkaclient.json.KafkaJsonSerializer;
import com.fc.kafkaclient.producer.KafkaProducer;
import com.fc.kafkaclient.test.model.DataStreamObj;

public class JsonSerializerDeserializerTest {

	public static final String TOPIC = KafkaClientConfig.loadConfig().getProperty("kafka.topic");
	public static final String DATAKEY = "TestKey";
	public static final DataStreamObj DATAVALUE = new DataStreamObj(1, "This is test data");
	
	@Test
	public void jsonTest() throws Exception {

		Map<String, DataStreamObj> returnedData = null;
		
		try (KafkaProducer<String, DataStreamObj> producer = KafkaProducer.build(TOPIC, new StringSerializer(),
				new KafkaJsonSerializer<DataStreamObj>())) {
			producer.sendMessage(TOPIC, true, DATAKEY, DATAVALUE);
		}
		
		try (KafkaConsumer<String, DataStreamObj> consumer = KafkaConsumer.build(TOPIC, new StringDeserializer(),
				new KafkaJsonDeserializer<DataStreamObj>(DataStreamObj.class))) {
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
