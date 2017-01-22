package com.orhundalabasmaz.storm.model;

/**
 * @author Orhun Dalabasmaz
 */
public abstract class Message {
	private String messageId;
	private String messageTime;

	public Message(String messageId, String messageTime) {
		this.messageId = messageId;
		this.messageTime = messageTime;
	}

	public String getMessageId() {
		return messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public String getMessageTime() {
		return messageTime;
	}

	public void setMessageTime(String messageTime) {
		this.messageTime = messageTime;
	}

	public String getKey() {
		return messageId;
	}
}
