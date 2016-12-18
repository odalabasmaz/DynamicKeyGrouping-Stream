package com.orhundalabasmaz.storm;

import com.orhundalabasmaz.storm.producer.FileProducer;
import com.orhundalabasmaz.storm.producer.InteractiveProducer;
import com.orhundalabasmaz.storm.producer.RandomProducer;
import com.orhundalabasmaz.storm.producer.StreamProducer;

public class Application {

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
		if (args.length != 2) {
			throw new Exception("exactly two params needed");
		}

		final String type = args[0];
		final String topicName = args[1];

		StreamProducer producer;
		if ("0".equals(type)) {
			producer = new RandomProducer(topicName);
		} else if ("1".equals(type)) {
			producer = new InteractiveProducer(topicName);
		} else if ("2".equals(type)) {
			producer = new FileProducer(topicName);
		} else {
			throw new Exception("unexpected type: " + type);
		}

		long begin = System.currentTimeMillis();
		producer.produceStream();
		long end = System.currentTimeMillis();
		long duration = (end - begin) / 1000;
		System.out.println("Time consumed: " + duration + " sec");
	}

}