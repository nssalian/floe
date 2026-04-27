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

package com.floe.core.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ApiKeyGenerator")
class ApiKeyGeneratorTest {

    @Nested
    @DisplayName("generateKey")
    class GenerateKey {

        @Test
        @DisplayName("should generate key with correct prefix")
        void shouldGenerateKeyWithPrefix() {
            String key = ApiKeyGenerator.generateKey();
            assertThat(key).startsWith(ApiKey.KEY_PREFIX);
        }

        @Test
        @DisplayName("should generate key with correct length")
        void shouldGenerateKeyWithCorrectLength() {
            String key = ApiKeyGenerator.generateKey();
            int expectedLength = ApiKey.KEY_PREFIX.length() + ApiKey.KEY_RANDOM_LENGTH;
            assertThat(key).hasSize(expectedLength);
        }

        @Test
        @DisplayName("should generate unique keys")
        void shouldGenerateUniqueKeys() {
            Set<String> keys = new HashSet<>();
            for (int i = 0; i < 1000; i++) {
                String key = ApiKeyGenerator.generateKey();
                assertThat(keys.add(key)).as("Key %d should be unique", i).isTrue();
            }
        }

        @Test
        @DisplayName("should only use alphanumeric characters")
        void shouldOnlyUseAlphanumericCharacters() {
            String key = ApiKeyGenerator.generateKey();
            String randomPart = key.substring(ApiKey.KEY_PREFIX.length());
            assertThat(randomPart).matches("[A-Za-z0-9]+");
        }
    }

    @Nested
    @DisplayName("hashKey")
    class HashKey {

        @Test
        @DisplayName("should produce consistent hash for same input")
        void shouldProduceConsistentHash() {
            String key = "floe_testkey12345678901234567890ab";
            String hash1 = ApiKeyGenerator.hashKey(key);
            String hash2 = ApiKeyGenerator.hashKey(key);
            assertThat(hash1).isEqualTo(hash2);
        }

        @Test
        @DisplayName("should produce different hash for different inputs")
        void shouldProduceDifferentHashForDifferentInputs() {
            String key1 = "floe_testkey12345678901234567890ab";
            String key2 = "floe_testkey12345678901234567890cd";
            String hash1 = ApiKeyGenerator.hashKey(key1);
            String hash2 = ApiKeyGenerator.hashKey(key2);
            assertThat(hash1).isNotEqualTo(hash2);
        }

        @Test
        @DisplayName("should produce 64-character hex hash (SHA-256)")
        void shouldProduce64CharHexHash() {
            String key = ApiKeyGenerator.generateKey();
            String hash = ApiKeyGenerator.hashKey(key);
            assertThat(hash).hasSize(64);
            assertThat(hash).matches("[a-f0-9]+");
        }

        @Test
        @DisplayName("should throw for null input")
        void shouldThrowForNullInput() {
            assertThatThrownBy(() -> ApiKeyGenerator.hashKey(null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("null or blank");
        }

        @Test
        @DisplayName("should throw for blank input")
        void shouldThrowForBlankInput() {
            assertThatThrownBy(() -> ApiKeyGenerator.hashKey("   "))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("null or blank");
        }

        @Test
        @DisplayName("should throw for empty input")
        void shouldThrowForEmptyInput() {
            assertThatThrownBy(() -> ApiKeyGenerator.hashKey(""))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("null or blank");
        }
    }

    @Nested
    @DisplayName("isValidKeyFormat")
    class IsValidKeyFormat {

        @Test
        @DisplayName("should return true for valid generated key")
        void shouldReturnTrueForValidGeneratedKey() {
            String key = ApiKeyGenerator.generateKey();
            assertThat(ApiKeyGenerator.isValidKeyFormat(key)).isTrue();
        }

        @Test
        @DisplayName("should return false for null")
        void shouldReturnFalseForNull() {
            assertThat(ApiKeyGenerator.isValidKeyFormat(null)).isFalse();
        }

        @Test
        @DisplayName("should return false for empty string")
        void shouldReturnFalseForEmptyString() {
            assertThat(ApiKeyGenerator.isValidKeyFormat("")).isFalse();
        }

        @Test
        @DisplayName("should return false for key without prefix")
        void shouldReturnFalseForKeyWithoutPrefix() {
            assertThat(ApiKeyGenerator.isValidKeyFormat("abc123456789012345678901234567890ab"))
                    .isFalse();
        }

        @Test
        @DisplayName("should return false for key with wrong prefix")
        void shouldReturnFalseForKeyWithWrongPrefix() {
            assertThat(ApiKeyGenerator.isValidKeyFormat("api_abc12345678901234567890123456"))
                    .isFalse();
        }

        @Test
        @DisplayName("should return false for key that is too short")
        void shouldReturnFalseForKeyTooShort() {
            assertThat(ApiKeyGenerator.isValidKeyFormat("floe_abc123")).isFalse();
        }

        @Test
        @DisplayName("should return false for key that is too long")
        void shouldReturnFalseForKeyTooLong() {
            assertThat(
                            ApiKeyGenerator.isValidKeyFormat(
                                    "floe_abc123456789012345678901234567890extra"))
                    .isFalse();
        }

        @Test
        @DisplayName("should return false for key with invalid characters")
        void shouldReturnFalseForKeyWithInvalidCharacters() {
            assertThat(ApiKeyGenerator.isValidKeyFormat("floe_abc123456789012345678901234!@#"))
                    .isFalse();
        }

        @Test
        @DisplayName("should return false for key with spaces")
        void shouldReturnFalseForKeyWithSpaces() {
            assertThat(ApiKeyGenerator.isValidKeyFormat("floe_abc 12345678901234567890123456"))
                    .isFalse();
        }
    }

    @Nested
    @DisplayName("Integration")
    class Integration {

        @Test
        @DisplayName("generated key should pass format validation")
        void generatedKeyShouldPassFormatValidation() {
            for (int i = 0; i < 100; i++) {
                String key = ApiKeyGenerator.generateKey();
                assertThat(ApiKeyGenerator.isValidKeyFormat(key))
                        .as("Generated key %d should be valid", i)
                        .isTrue();
            }
        }

        @Test
        @DisplayName("generated key should be hashable")
        void generatedKeyShouldBeHashable() {
            String key = ApiKeyGenerator.generateKey();
            String hash = ApiKeyGenerator.hashKey(key);
            assertThat(hash).isNotNull().hasSize(64);
        }
    }
}
