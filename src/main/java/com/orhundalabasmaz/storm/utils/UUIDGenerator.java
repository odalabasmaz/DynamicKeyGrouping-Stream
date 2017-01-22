package com.orhundalabasmaz.storm.utils;

import java.util.UUID;

/**
 * @author Orhun Dalabasmaz
 */
public final class UUIDGenerator {

	public static String generateUUID() {
		final UUID uuid = UUID.randomUUID();
		return uuid.toString().toUpperCase();
	}
}