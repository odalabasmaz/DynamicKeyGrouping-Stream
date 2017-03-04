package com.orhundalabasmaz.storm;

import com.orhundalabasmaz.storm.kafka.producer.FileProducer;
import com.orhundalabasmaz.storm.kafka.producer.InteractiveProducer;
import com.orhundalabasmaz.storm.kafka.producer.RandomProducer;
import com.orhundalabasmaz.storm.kafka.producer.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {
	private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

	private Application() {
	}

	/**
	 * Run the program as;
	 * $ java -jar dkg-stream-wd.jar <type> <topic_name>
	 * <p>
	 * type: 0,1,2
	 * 0 - Random
	 * 1 - Interactive
	 * 2 - File
	 * <p>
	 * topic_name: topic1
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			LOGGER.error("at least two params needed");
			throw new UnsupportedOperationException("at least two params needed");
		}

		final String type = args[0];
		final String topicName = args[1];

		StreamProducer producer;
		if ("0".equals(type)) {
			producer = new RandomProducer(topicName);
		} else if ("1".equals(type)) {
			producer = new InteractiveProducer(topicName);
		} else if ("2".equals(type)) {
			if (args.length >= 3) {
				producer = new FileProducer(topicName, args[2]);
			} else {
				producer = new FileProducer(topicName);
			}
		} else {
			LOGGER.error("unexpected type: {}", type);
			throw new UnsupportedOperationException("unexpected type: " + type);
		}

		long begin = System.currentTimeMillis();
		producer.produceStream();
		long end = System.currentTimeMillis();
		long duration = (end - begin) / 1000;
		LOGGER.info("Time consumed: {} sec", duration);
	}
}