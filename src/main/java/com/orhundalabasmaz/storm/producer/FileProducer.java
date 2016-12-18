package com.orhundalabasmaz.storm.producer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Orhun Dalabasmaz
 */
public class FileProducer extends BaseProducer {

	private final String filePath;

	public FileProducer(String topicName) {
		super(topicName);
		this.filePath = "data/country.txt";
	}

	public FileProducer(String topicName, String filePath) {
		super(topicName);
		this.filePath = filePath;
	}

	@Override
	protected void produce() {
		try {
			URL resource = this.getClass().getClassLoader().getResource(filePath);
			if (resource == null) {
				throw new FileNotFoundException("File not found! " + filePath);
			}
			Path path = Paths.get(resource.toURI());

			// method 1
			/*List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
			for (String line : lines) {
				sendMessage(line);
			}*/

			// method 2
			try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
				String line;
				while ((line = br.readLine()) != null) {
					sendMessage(line.trim());
				}
			}

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

		} catch (IOException | URISyntaxException e) {
			System.err.println("Exception occurred: " + e.getMessage());
			e.printStackTrace();
		}
	}

}
