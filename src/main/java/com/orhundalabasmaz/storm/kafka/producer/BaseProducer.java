package com.orhundalabasmaz.storm.kafka.producer;

import com.orhundalabasmaz.storm.model.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author Orhun Dalabasmaz
 */
public abstract class BaseProducer<M extends Message> implements StreamProducer {
	private Logger LOGGER = LoggerFactory.getLogger(BaseProducer.class);

	private Producer producer;
	private final String topicName;
//	private final String servers = "85.110.34.250:9092";
//	private final String servers = "localhost:9092";
	private final String servers = "192.168.1.39:9092";

	protected BaseProducer(String topicName) {
		this.topicName = topicName;
		init();
	}

	private void init() {
		Properties configProperties = new Properties();
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.orhundalabasmaz.storm.serializer.JsonSerializer");
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
		LOGGER.info("Producing stream ...");
		produce();
		producer.close();
		LOGGER.info("Producing done ...");
	}

	protected abstract void produce();

	protected final void sendMessage(M message) {
		ProducerRecord<String, String> rec = new ProducerRecord(topicName, message.getKey(), message.getKey());
		producer.send(rec);
//		producer.flush();
	}

}
