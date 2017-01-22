package com.orhundalabasmaz.storm.producer;

import com.orhundalabasmaz.storm.model.CountryMessage;
import com.orhundalabasmaz.storm.utils.UUIDGenerator;

import java.util.Scanner;

/**
 * @author Orhun Dalabasmaz
 */
public class InteractiveProducer extends BaseProducer<CountryMessage> {

	public InteractiveProducer(String topicName) {
		super(topicName);
	}

	@Override
	public void produce() {
		Scanner in = new Scanner(System.in);
		String line;
		System.out.println("Enter message(type exit to quit)");
		System.out.print("> ");
		line = in.nextLine();
		while (!line.equals("exit")) {
			CountryMessage message = new CountryMessage(UUIDGenerator.generateUUID(), String.valueOf(System.currentTimeMillis()));
			message.setCountryCode("N/A");
			message.setCountryName(line.trim());
			sendMessage(message);
			System.out.print("> ");
			line = in.nextLine();
		}
		in.close();
	}
}
