package com.orhundalabasmaz.storm.kafka.producer;

import com.orhundalabasmaz.storm.model.CountryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Orhun Dalabasmaz
 */
public class FileProducer extends BaseProducer<CountryMessage> {
	private static final Logger LOGGER = LoggerFactory.getLogger(FileProducer.class);
	private final String filePath;

	public FileProducer(String topicName) {
		super(topicName);
		this.filePath = "data/country-skew.txt";
	}

	public FileProducer(String topicName, String filePath) {
		super(topicName);
		this.filePath = filePath;
	}

	@Override
	protected void produce() {
		try {
			/*URL resource = this.getClass().getClassLoader().getResource(filePath);
			if (resource == null) {
				LOGGER.error("File not found! {}", filePath);
				throw new FileNotFoundException("File not found! " + filePath);
			}*/
			Path path = Paths.get(filePath);

			// method 1
			/*List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
			for (String line : lines) {
				sendMessage(line);
			}*/

			// method 2
			long count = 0;
			Map<String, Integer> map = new HashMap<>();
			try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
				String line;
				while ((line = br.readLine()) != null) {
					String countryName = line.trim();
					CountryMessage message = new CountryMessage(countryName, String.valueOf(System.currentTimeMillis()));
					message.setCountryCode("N/A");
					message.setCountryName(countryName);
					sendMessage(message);

					if (!map.containsKey(countryName)) {
						map.put(countryName, 0);
					}
					map.put(countryName, map.get(countryName) + 1);
					++count;
				}
			}
			LOGGER.info(">>> Total produced: {}", count);
			String result = "\ncountry,count";
			TreeMap<String, Integer> treeMap = new TreeMap<>(map);
			for (Map.Entry<String, Integer> entry : treeMap.entrySet()) {
				String key = entry.getKey();
				Integer value = entry.getValue();
				result += "\n" + key + "," + value;
			}
			LOGGER.info(result);

			// method 3
			/*FileInputStream fileInputStream = new FileInputStream(path.toFile());
			FileChannel channel = fileInputStream.getChannel();
			MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
			buffer.load();
			for (int i = 0; i < buffer.limit(); i++) {
				System.out.println((char) buffer.get());
			}
			buffer.clear();
			fileInputStream.close();
			channel.close();*/

		} catch (IOException e) {
			LOGGER.error("Exception occurred.", e);
		}
	}

}
