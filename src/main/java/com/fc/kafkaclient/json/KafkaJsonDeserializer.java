package com.fc.kafkaclient.json;

import java.util.logging.Level;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fc.kafkaclient.KafkaClientConfig;

public class KafkaJsonDeserializer<T> implements Deserializer<T> {
	
	private Class <T> type;
	
	public KafkaJsonDeserializer(Class<T> type) {
		this.type = type;
	}

	public T deserialize(String topic, byte[] data) {
		T result = null;
		ObjectMapper mapper = new ObjectMapper();
		try {
			result = mapper.readValue(data, this.type);
		} catch(Exception e) {
			KafkaClientConfig.LOGGER.log(Level.SEVERE, "Failed to searilize [" + data.toString() + "]", e);
		}
		return result;
	}
}
