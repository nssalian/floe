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

package com.floe.server.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.floe.core.auth.*;
import com.floe.server.config.FloeConfig;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@DisplayName("ApiKeyIdentityProvider")
class ApiKeyIdentityProviderTest {

    @Mock private ApiKeyStore apiKeyStore;

    @Mock private FloeConfig config;

    @Mock private FloeConfig.Auth auth;

    @Mock private AuditLogger auditLogger;

    @Mock private AuthenticationRequest request;

    private ApiKeyIdentityProvider provider;

    private static final String HEADER_NAME = "X-API-Key";
    private static final String CLIENT_IP = "192.168.1.100";
    private static final String REQUEST_PATH = "/api/v1/policies";
    private static final String REQUEST_METHOD = "GET";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        when(config.auth()).thenReturn(auth);
        when(auth.headerName()).thenReturn(HEADER_NAME);
        when(request.getClientIp()).thenReturn(CLIENT_IP);
        when(request.getPath()).thenReturn(REQUEST_PATH);
        when(request.getMethod()).thenReturn(REQUEST_METHOD);

        provider = new ApiKeyIdentityProvider();
        provider.apiKeyStore = apiKeyStore;
        provider.config = config;
        provider.auditLogger = auditLogger;
    }

    @Nested
    @DisplayName("supports")
    class Supports {

        @Test
        @DisplayName("should return true when API key header is present")
        void shouldReturnTrueWhenApiKeyPresent() {
            when(request.getApiKey(HEADER_NAME)).thenReturn(Optional.of("floe_testkey"));
            assertThat(provider.supports(request)).isTrue();
        }

        @Test
        @DisplayName("should return false when API key header is missing")
        void shouldReturnFalseWhenApiKeyMissing() {
            when(request.getApiKey(HEADER_NAME)).thenReturn(Optional.empty());
            assertThat(provider.supports(request)).isFalse();
        }
    }

    @Nested
    @DisplayName("authenticate")
    class Authenticate {

        private ApiKey validApiKey;
        private String plaintextKey;
        private String keyHash;

        @BeforeEach
        void setUp() {
            plaintextKey = "floe_validkey12345678901234567890";
            keyHash = ApiKeyGenerator.hashKey(plaintextKey);

            validApiKey =
                    ApiKey.builder()
                            .id("key-123")
                            .keyHash(keyHash)
                            .name("Test Key")
                            .role(Role.OPERATOR)
                            .enabled(true)
                            .createdBy("admin")
                            .build();
        }

        @Test
        @DisplayName("should return empty when no API key in request")
        void shouldReturnEmptyWhenNoApiKey() {
            when(request.getApiKey(HEADER_NAME)).thenReturn(Optional.empty());

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isEmpty();
            verifyNoInteractions(apiKeyStore);
        }

        @Test
        @DisplayName("should authenticate valid API key")
        void shouldAuthenticateValidApiKey() {
            when(request.getApiKey(HEADER_NAME)).thenReturn(Optional.of(plaintextKey));
            when(apiKeyStore.findByKeyHash(keyHash)).thenReturn(Optional.of(validApiKey));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            FloePrincipal principal = result.get();
            assertThat(principal.userId()).isEqualTo("key-123");
            assertThat(principal.username()).isEqualTo("Test Key");
            assertThat(principal.roles()).containsExactly(Role.OPERATOR);
            assertThat(principal.authenticationMethod()).isEqualTo("API_KEY");
        }

        @Test
        @DisplayName("should return empty when API key not found")
        void shouldReturnEmptyWhenKeyNotFound() {
            when(request.getApiKey(HEADER_NAME)).thenReturn(Optional.of(plaintextKey));
            when(apiKeyStore.findByKeyHash(keyHash)).thenReturn(Optional.empty());

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isEmpty();
            verify(auditLogger)
                    .log(
                            AuditEvent.AUTH_FAILED_INVALID_KEY,
                            CLIENT_IP,
                            REQUEST_PATH,
                            REQUEST_METHOD);
        }

        @Test
        @DisplayName("should return empty when API key is disabled")
        void shouldReturnEmptyWhenKeyDisabled() {
            ApiKey disabledKey = validApiKey.withEnabled(false);
            when(request.getApiKey(HEADER_NAME)).thenReturn(Optional.of(plaintextKey));
            when(apiKeyStore.findByKeyHash(keyHash)).thenReturn(Optional.of(disabledKey));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isEmpty();
            verify(auditLogger)
                    .log(
                            AuditEvent.AUTH_FAILED_DISABLED_KEY,
                            "key-123",
                            "Test Key",
                            CLIENT_IP,
                            REQUEST_PATH);
        }

        @Test
        @DisplayName("should return empty when API key is expired")
        void shouldReturnEmptyWhenKeyExpired() {
            ApiKey expiredKey =
                    new ApiKey(
                            validApiKey.id(),
                            validApiKey.keyHash(),
                            validApiKey.name(),
                            validApiKey.role(),
                            validApiKey.enabled(),
                            validApiKey.createdAt(),
                            Instant.now().minus(1, ChronoUnit.DAYS), // expired yesterday
                            validApiKey.lastUsedAt(),
                            validApiKey.createdBy());

            when(request.getApiKey(HEADER_NAME)).thenReturn(Optional.of(plaintextKey));
            when(apiKeyStore.findByKeyHash(keyHash)).thenReturn(Optional.of(expiredKey));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isEmpty();
            verify(auditLogger)
                    .log(
                            AuditEvent.AUTH_FAILED_EXPIRED_KEY,
                            "key-123",
                            "Test Key",
                            CLIENT_IP,
                            REQUEST_PATH);
        }

        @Test
        @DisplayName("should allow non-expiring key (null expiresAt)")
        void shouldAllowNonExpiringKey() {
            when(request.getApiKey(HEADER_NAME)).thenReturn(Optional.of(plaintextKey));
            when(apiKeyStore.findByKeyHash(keyHash)).thenReturn(Optional.of(validApiKey));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
        }

        @Test
        @DisplayName("should allow key with future expiration")
        void shouldAllowKeyWithFutureExpiration() {
            ApiKey futureExpiringKey =
                    new ApiKey(
                            validApiKey.id(),
                            validApiKey.keyHash(),
                            validApiKey.name(),
                            validApiKey.role(),
                            validApiKey.enabled(),
                            validApiKey.createdAt(),
                            Instant.now().plus(30, ChronoUnit.DAYS), // expires in 30 days
                            validApiKey.lastUsedAt(),
                            validApiKey.createdBy());

            when(request.getApiKey(HEADER_NAME)).thenReturn(Optional.of(plaintextKey));
            when(apiKeyStore.findByKeyHash(keyHash)).thenReturn(Optional.of(futureExpiringKey));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
        }

        @Test
        @DisplayName("should log successful authentication")
        void shouldLogSuccessfulAuth() {
            when(request.getApiKey(HEADER_NAME)).thenReturn(Optional.of(plaintextKey));
            when(apiKeyStore.findByKeyHash(keyHash)).thenReturn(Optional.of(validApiKey));

            provider.authenticate(request);

            verify(auditLogger)
                    .log(AuditEvent.AUTH_SUCCESS, "key-123", "Test Key", CLIENT_IP, REQUEST_PATH);
        }

        @Test
        @DisplayName("should include metadata in principal")
        void shouldIncludeMetadataInPrincipal() {
            when(request.getApiKey(HEADER_NAME)).thenReturn(Optional.of(plaintextKey));
            when(apiKeyStore.findByKeyHash(keyHash)).thenReturn(Optional.of(validApiKey));

            Optional<FloePrincipal> result = provider.authenticate(request);

            assertThat(result).isPresent();
            FloePrincipal principal = result.get();
            assertThat(principal.metadata()).containsKey("createdBy");
            assertThat(principal.metadata()).containsKey("createdAt");
        }
    }

    @Nested
    @DisplayName("properties")
    class Properties {

        @Test
        @DisplayName("should return ApiKey scheme")
        void shouldReturnApiKeyScheme() {
            assertThat(provider.getScheme()).isEqualTo("ApiKey");
        }

        @Test
        @DisplayName("should have priority 50")
        void shouldHavePriority50() {
            assertThat(provider.priority()).isEqualTo(50);
        }
    }
}
