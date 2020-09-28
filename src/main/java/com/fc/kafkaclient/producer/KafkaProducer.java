package com.fc.kafkaclient.producer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.logging.Level;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

import com.fc.kafkaclient.KafkaClientConfig;
import com.fc.kafkaclient.KafkaClientException;

public class KafkaProducer<K, V> implements AutoCloseable {

	protected Producer<K, V> producer;
	protected List<String> topics;
	protected Properties properties;
	protected Serializer<K> keySerializer;
	protected Serializer<V> valueSerializer;
	
	private KafkaProducer(List<String> topics, Properties properties) {
		this.topics = topics;
		this.properties = properties;
	}

	protected void setKeySerializer(Serializer<K> keySerializer) {
		this.keySerializer = keySerializer;
	}
	
	protected void setValueSerializer(Serializer<V> valueSerializer) {
		this.valueSerializer = valueSerializer;
	}
	
	protected void init() {
		producer = new org.apache.kafka.clients.producer.KafkaProducer<K, V>(properties, this.keySerializer, this.valueSerializer);
	}
	
	protected void sendKafkaMessage(String topic, boolean async, K key, V value) {
		try {
			if(!topics.contains(topic)) {
				throw new KafkaClientException("No such topic exists", topic);
			}
			
			ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
			Future<RecordMetadata> response = producer.send(record);
			if(async) {
				response.get();
			}
		} catch(Exception e) {
			KafkaClientConfig.LOGGER.log(Level.SEVERE, "Error occured while sending the message", e);
			throw new KafkaClientException("Error occured while sending the message", e);
		}
	}
	
	public void sendMessage(String topic, boolean async, K key, V value) {
		sendKafkaMessage(topic, async, key, value);
	}
	
	public void sendMessage(String topic, K key, V value) {
		sendKafkaMessage(topic, false, key, value);
	}
	
	@Override
	public void close() throws Exception {
		if(producer != null) {
			producer.close();
		}
	}
	
	public static <K, V> KafkaProducer<K, V> build(String topic, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
		return build(topic, KafkaClientConfig.loadConfig(), keySerializer, valueSerializer);
	}
	
	public static <K, V> KafkaProducer<K, V> build(String topic, Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
		return KafkaProducer.build(Arrays.asList(topic), properties, keySerializer, valueSerializer);
	}
	
	public static <K, V> KafkaProducer<K, V> build(List<String> topics, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
		return build(topics, KafkaClientConfig.loadConfig(), keySerializer, valueSerializer);
	}
	
	public static <K, V> KafkaProducer<K, V> build(List<String> topics, Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
		try {
			KafkaProducer<K, V> kafkaProducer = new KafkaProducer<>(topics, properties);
			kafkaProducer.setKeySerializer(keySerializer);
			kafkaProducer.setValueSerializer(valueSerializer);
			kafkaProducer.init();
			return kafkaProducer;
		} catch(Exception e) {
			KafkaClientConfig.LOGGER.log(Level.SEVERE, "Failed to build KafkaProducer", e);
			throw new KafkaClientException("Failed to build KafkaProducer", e);
		}
	}

}
