package com.fc.kafkaclient.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Deserializer;

import com.fc.kafkaclient.KafkaClientConfig;
import com.fc.kafkaclient.KafkaClientException;

public class KafkaConsumer<K, V> implements AutoCloseable {

	protected Consumer<K, V> consumer;
	protected List<String> topics;
	protected Properties properties;
	protected Deserializer<K> keyDeserializer;
	protected Deserializer<V> valueDeserializer;
	
	private KafkaConsumer(List<String> topics, Properties properties) {
		this.topics = topics;
		this.properties = properties;
	}

	protected void setKeyDeserializer(Deserializer<K> keyDeserializer) {
		this.keyDeserializer = keyDeserializer;
	}
	
	protected void setValueDeserializer(Deserializer<V> valueDeserializer) {
		this.valueDeserializer = valueDeserializer;
	}
	
	protected void init() {
		consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<K, V>(properties, this.keyDeserializer, this.valueDeserializer);
		consumer.subscribe(this.topics);
	}
	
	public Map<K, V> readAsMap(){
		return readAsMap(0);
	}
	
	public Map<K, V> readAsMap(int time) {
		Map<K, V> result = new HashMap<>();
		ConsumerRecords<K, V> records = read(time);
		for(ConsumerRecord<K, V> record : records) {
			result.put(record.key(), record.value());
		}
		return result;
	}
	
	public ConsumerRecords<K, V> read(){
		return read(0);
	}
	
	@Override
	public void close() throws Exception {
		if(consumer != null) {
			consumer.close();
		}
	}
	
	public ConsumerRecords<K, V> read(int time) {
		ConsumerRecords<K, V> record = consumer.poll(Duration.ofMillis(time == 0 ? 1000 : time));
		consumer.commitAsync();
		return record;
	}
	
	public static <K, V> KafkaConsumer<K, V> build(String topic, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
		return build(topic, KafkaClientConfig.loadConfig(), keyDeserializer, valueDeserializer);
	}
	
	public static <K, V> KafkaConsumer<K, V> build(String topic, Properties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
		return KafkaConsumer.build(Arrays.asList(topic), properties, keyDeserializer, valueDeserializer);
	}
	
	public static <K, V> KafkaConsumer<K, V> build(List<String> topics, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
		return build(topics, KafkaClientConfig.loadConfig(), keyDeserializer, valueDeserializer);
	}
	
	public static <K, V> KafkaConsumer<K, V> build(List<String> topics, Properties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
		try {
			KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<>(topics, properties);
			kafkaConsumer.setKeyDeserializer(keyDeserializer);
			kafkaConsumer.setValueDeserializer(valueDeserializer);
			kafkaConsumer.init();
			return kafkaConsumer;
		} catch(Exception e) {
			KafkaClientConfig.LOGGER.log(Level.SEVERE, "Failed to build KafkaConsumer", e);
			throw new KafkaClientException("Failed to build KafkaConsumer", e);
		}
	}

}
