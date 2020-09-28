package com.fc.kafkaclient.json;

import java.util.logging.Level;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fc.kafkaclient.KafkaClientConfig;

public class KafkaJsonSerializer<T> implements Serializer<T> {

	public byte[] serialize(String topic, Object data) {
		byte[] result = null;
		
		ObjectMapper mapper = new ObjectMapper();
		try {
			result = mapper.writeValueAsBytes(data);
		} catch(Exception e) {
			KafkaClientConfig.LOGGER.log(Level.SEVERE, "Failed to searilize [" + data.toString() + "]", e);
		}
		
		return result;
	}

}
