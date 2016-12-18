package com.orhundalabasmaz.storm.producer;

import java.util.Scanner;

/**
 * @author Orhun Dalabasmaz
 */
public class InteractiveProducer extends BaseProducer {

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
			sendMessage(line);
			System.out.print("> ");
			line = in.nextLine();
		}
		in.close();
	}
}
