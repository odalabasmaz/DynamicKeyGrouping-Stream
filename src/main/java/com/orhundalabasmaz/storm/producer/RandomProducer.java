package com.orhundalabasmaz.storm.producer;

import java.util.Random;

/**
 * @author Orhun Dalabasmaz
 */
public class RandomProducer extends BaseProducer {

	private long limit = 10_000;
	private final Random random = new Random();

	public RandomProducer(String topicName) {
		super(topicName);
	}

	public RandomProducer(String topicName, long limit) {
		super(topicName);
		this.limit = limit;
	}

	@Override
	protected void produce() {
		for (int i = 0; i < limit; ++i) {
			sendMessage(rand());
		}
	}

	private String rand() {
		return String.valueOf(random.nextInt(10));
	}
}
