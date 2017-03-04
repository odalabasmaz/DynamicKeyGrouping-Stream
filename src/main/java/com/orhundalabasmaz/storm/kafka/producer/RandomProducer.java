package com.orhundalabasmaz.storm.kafka.producer;

import com.orhundalabasmaz.storm.model.CountryMessage;
import com.orhundalabasmaz.storm.utils.UUIDGenerator;

import java.util.Random;

/**
 * @author Orhun Dalabasmaz
 */
public class RandomProducer extends BaseProducer<CountryMessage> {

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
			CountryMessage message = new CountryMessage(UUIDGenerator.generateUUID(), String.valueOf(System.currentTimeMillis()));
			message.setCountryCode("N/A");
			message.setCountryName(rand().trim());
			sendMessage(message);
		}
	}

	private String rand() {
		return String.valueOf(random.nextInt(10));
	}
}
