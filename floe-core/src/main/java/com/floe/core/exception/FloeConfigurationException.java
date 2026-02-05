/*
 * Copyright 2026 The Floe Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
