package com.orhundalabasmaz.storm.model;

/**
 * @author Orhun Dalabasmaz
 */
public class CountryMessage extends Message {
	private String countryCode;
	private String countryName;

	public CountryMessage(String messageId, String messageTime) {
		super(messageId, messageTime);
	}

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	public String getCountryName() {
		return countryName;
	}

	public void setCountryName(String countryName) {
		this.countryName = countryName;
	}
}
