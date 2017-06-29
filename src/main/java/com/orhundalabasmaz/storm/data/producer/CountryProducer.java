package com.orhundalabasmaz.storm.data.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * @author Orhun Dalabasmaz
 */
public class CountryProducer {
	private static final Logger LOGGER = LoggerFactory.getLogger(CountryProducer.class);
	private final Random random = new Random();
	private Map<String, Integer> countryNames = new HashMap<>();
	private FileWriter fileWriter;
	private BufferedWriter bufferedWriter;

	public CountryProducer(String fileName) {
		try {
			this.fileWriter = new FileWriter(fileName, false);
			this.bufferedWriter = new BufferedWriter(fileWriter);
		} catch (IOException e) {
			LOGGER.error("Exception occurred!", e);
		}
	}

	public void produceHalfSkewed() {
		for (int i = 0; i < 5_000_000; ++i) {
			writeToFile(skewed());
		}

		for (int i = 0; i < 5_000_000; ++i) {
			writeToFile(balanced());
		}

		endOfLine();
	}

	public void produceBalanced() {
		for (int i = 0; i < 10_000_000; ++i) {
			writeToFile(balanced());
		}

		endOfLine();
	}

	private void endOfLine() {
		TreeMap<String, Integer> map = new TreeMap<>(countryNames);
		StringBuilder result = new StringBuilder();
		result.append("Countries (").append(countryNames.size()).append(")\n");
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			result.append(entry.getKey()).append(", ").append(entry.getValue()).append("\n");
		}
		try {
			bufferedWriter.close();
			fileWriter.close();
		} catch (IOException e) {
			LOGGER.error("Close ex", e);
		}
		LOGGER.info(result.toString());
	}

	private void writeToFile(String name) {
		addToMap(name);
		try {
			bufferedWriter.write(name);
			bufferedWriter.newLine();
		} catch (IOException e) {
			LOGGER.error("Exception occurred when writing to file:", e);
		}
	}

	private void addToMap(String name) {
		Integer count = countryNames.get(name);
		if (count == null) {
			count = 0;
		}
		countryNames.put(name, count + 1);
	}

	private String balanced() {
		int i = random.nextInt(30);
		return Data.restrictedCountryNames.get(i);
	}

	private String skewed() {
		int p = random.nextInt(100);
		if (p < 80) {
			return "turkey";
		} else if (p < 90) {
			return "spain";
		}
		int i = random.nextInt(30);
		return Data.restrictedCountryNames.get(i);
	}
}
