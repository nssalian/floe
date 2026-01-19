package com.floe.core.exception;

/**
 * Exception thrown when configuration or serialization fails.
 *
 * <p>Used for JSON parsing errors, invalid configuration values, and similar issues.
 */
public class FloeConfigurationException extends RuntimeException {

    private final String configKey;
    private final Object invalidValue;

    public FloeConfigurationException(String message, Throwable cause) {
        super(message, cause);
        this.configKey = null;
        this.invalidValue = null;
    }

    public FloeConfigurationException(String configKey, Object invalidValue, Throwable cause) {
        super(buildMessage(configKey, invalidValue, cause), cause);
        this.configKey = configKey;
        this.invalidValue = invalidValue;
    }

    private static String buildMessage(String configKey, Object invalidValue, String reason) {
        return String.format(
                "Invalid configuration for '%s' with value '%s': %s",
                configKey, invalidValue, reason);
    }

    private static String buildMessage(String configKey, Object invalidValue, Throwable cause) {
        String msg =
                String.format(
                        "Invalid configuration for '%s' with value '%s'", configKey, invalidValue);
        if (cause != null && cause.getMessage() != null) {
            msg += ": " + cause.getMessage();
        }
        return msg;
    }
}
