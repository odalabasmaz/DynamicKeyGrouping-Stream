package com.orhundalabasmaz.storm.data.producer;

/**
 * @author Orhun Dalabasmaz
 */
public class DataApplication {
	public static void main(String... args) {
		CountryProducer producer = new CountryProducer("C:/data/cloud/synthetic/country-half-skew.txt");
		producer.produceHalfSkewed();

		producer = new CountryProducer("C:/data/cloud/synthetic/country-balanced.txt");
		producer.produceBalanced();
	}
}
