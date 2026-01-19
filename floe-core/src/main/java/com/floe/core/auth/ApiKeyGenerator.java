package com.floe.core.auth;

import com.floe.core.exception.FloeConfigurationException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HexFormat;

/** Utility class for generating and hashing API keys. */
public final class ApiKeyGenerator {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final String ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private ApiKeyGenerator() {}

    /**
     * Generate a new random API key.
     *
     * @return A new key in the format "floe_" + 32 random alphanumeric characters
     */
    public static String generateKey() {
        StringBuilder sb = new StringBuilder(ApiKey.KEY_PREFIX);
        for (int i = 0; i < ApiKey.KEY_RANDOM_LENGTH; i++) {
            int index = SECURE_RANDOM.nextInt(ALPHABET.length());
            sb.append(ALPHABET.charAt(index));
        }
        return sb.toString();
    }

    /**
     * Hash an API key using SHA-256.
     *
     * @param key The plaintext key to hash
     * @return The hex-encoded SHA-256 hash
     */
    public static String hashKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be null or blank");
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(key.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new FloeConfigurationException("SHA-256 algorithm not available", e);
        }
    }

    /**
     * Validate that a key has the correct format.
     *
     * @param key The key to validate
     * @return true if the key has valid format
     */
    public static boolean isValidKeyFormat(String key) {
        if (key == null) {
            return false;
        }
        if (!key.startsWith(ApiKey.KEY_PREFIX)) {
            return false;
        }
        String randomPart = key.substring(ApiKey.KEY_PREFIX.length());
        if (randomPart.length() != ApiKey.KEY_RANDOM_LENGTH) {
            return false;
        }
        for (char c : randomPart.toCharArray()) {
            if (ALPHABET.indexOf(c) < 0) {
                return false;
            }
        }
        return true;
    }
}
