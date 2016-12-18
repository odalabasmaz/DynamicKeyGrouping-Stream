package com.orhundalabasmaz.storm.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author Orhun Dalabasmaz
 */
public abstract class BaseProducer implements StreamProducer {

	private Producer producer;
	private final String topicName;
	private final String servers = "localhost:9092";

	protected BaseProducer(String topicName) {
		this.topicName = topicName;
		init();
	}

	private void init() {
		Properties configProperties = new Properties();
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		/*props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);*/
		producer = new KafkaProducer(configProperties);
	}

	public final void produceStream() {
		System.out.println("Producing stream ...");
		produce();
		producer.close();
		System.out.println("Producing done ...");
	}

	protected abstract void produce();

	protected final void sendMessage(String message) {
		ProducerRecord<String, String> rec = new ProducerRecord(topicName, message);
		producer.send(rec);
//		producer.flush();
	}

}
