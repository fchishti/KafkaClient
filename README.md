Kafka Client
=================

Lightweight Kafka client for producing and consuming datastream.

## Usage

Please take a look at the [test](src/test/java/com/fc/kafkaclient/test) classes for an example.

#### Producer

You should use the `sendMessage` method with `async` parameter set as false if you would like to not wait for producer to register a message with the Kafka server.

```java
import org.apache.kafka.common.serialization.StringSerializer;
import com.fc.kafkaclient.producer.KafkaProducer;

public class Producer {

	public void producer() {
		
		try (KafkaProducer<String, String> producer = KafkaProducer.build(TOPIC, new StringSerializer(),
				new StringSerializer())) {
			producer.sendMessage(TOPIC, false, DATAKEY, DATAVALUE);
		}
	}
}
```

#### Consumer

```java
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fc.kafkaclient.producer.KafkaConsumer;

public class Consumer {

	public void consumer() {
		
		try (KafkaConsumer<String, String> consumer = KafkaConsumer.build(TOPIC, new StringDeserializer(),
				new StringDeserializer())) {
			while(true) {
				consumer.read();
			}
		}
	}
}
```

### JSON Serializer and Deserializer

This is a generic JSON serializer and deserializer that can be used to produce and consume any JSON data.

#### JSON Serializer

```java
import org.apache.kafka.common.serialization.StringSerializer;
import com.fc.kafkaclient.json.KafkaJsonSerializer;
import com.fc.kafkaclient.producer.KafkaProducer;
import com.fc.kafkaclient.test.model.DataStreamObj;

public class Producer {

	public void producer() {
		
		try (KafkaProducer<String, DataStreamObj> producer = KafkaProducer.build(TOPIC, new StringSerializer(),
				new KafkaJsonSerializer<DataStreamObj>())) {
			producer.sendMessage(TOPIC, true, DATAKEY, DATAVALUE);
		}
	}
}
```

#### JSON Deserializer

```java
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fc.kafkaclient.json.KafkaJsonDeserializer;
import com.fc.kafkaclient.producer.KafkaConsumer;
import com.fc.kafkaclient.test.model.DataStreamObj;

public class Consumer {

	public void consumer() {
		
		try (KafkaConsumer<String, DataStreamObj> consumer = KafkaConsumer.build(TOPIC, new StringDeserializer(),
				new KafkaJsonDeserializer<DataStreamObj>(DataStreamObj.class))) {
			while(true) {
				consumer.read();
			}
		}
	}
}
```

## Future work
I plan to add more custom serializers and deserializers for XML objects and expand the client for more robust utilization. 
This repo is a work in progress and contributions will be enabled soon so please stay tuned!
