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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.floe.core.auth.*;
import com.floe.server.config.FloeConfig;
import com.floe.server.metrics.FloeMetrics;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class FloeHttpAuthenticationMechanismTest {

    @Mock private IdentityProvider provider1;

    @Mock private IdentityProvider provider2;

    @Mock private AuditLogger auditLogger;

    @Mock private FloeMetrics metrics;

    @Mock private FloeConfig config;

    @Mock private FloeConfig.Auth authConfig;

    @Mock private RoutingContext routingContext;

    @Mock private HttpServerRequest httpRequest;

    @Mock private IdentityProviderManager identityProviderManager;

    @Mock private SocketAddress remoteAddress;

    private FloeHttpAuthenticationMechanism mechanism;

    private void setupMechanism(List<IdentityProvider> providers) {
        mechanism = new FloeHttpAuthenticationMechanism(providers, auditLogger, metrics, config);
    }

    private void setupAuthConfig(boolean enabled) {
        lenient().when(config.auth()).thenReturn(authConfig);
        lenient().when(authConfig.enabled()).thenReturn(enabled);
    }

    private void setupRoutingContext() {
        lenient().when(routingContext.request()).thenReturn(httpRequest);
        lenient().when(httpRequest.path()).thenReturn("/api/v1/test");
        lenient().when(httpRequest.method()).thenReturn(HttpMethod.GET);
        lenient().when(httpRequest.remoteAddress()).thenReturn(remoteAddress);
        lenient().when(remoteAddress.host()).thenReturn("127.0.0.1");
    }

    @Nested
    class AuthenticationDisabled {

        @Test
        void shouldReturnNullWhenAuthDisabled() {
            setupAuthConfig(false);
            setupMechanism(List.of(provider1));

            SecurityIdentity result =
                    mechanism
                            .authenticate(routingContext, identityProviderManager)
                            .await()
                            .indefinitely();

            assertNull(result);
            verifyNoInteractions(provider1);
        }
    }

    @Nested
    class NoProviders {

        @Test
        void shouldReturnNullWhenNoProvidersRegistered() {
            setupAuthConfig(true);
            setupMechanism(List.of());

            SecurityIdentity result =
                    mechanism
                            .authenticate(routingContext, identityProviderManager)
                            .await()
                            .indefinitely();

            assertNull(result);
        }
    }

    @Nested
    class SuccessfulAuthentication {

        @BeforeEach
        void setUp() {
            setupAuthConfig(true);
            setupRoutingContext();
        }

        @Test
        void shouldAuthenticateWithFirstMatchingProvider() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-1")
                            .username("testuser")
                            .roles(Set.of(Role.VIEWER))
                            .authenticationMethod("API_KEY")
                            .build();

            when(provider1.priority()).thenReturn(100);
            when(provider1.supports(any())).thenReturn(true);
            when(provider1.authenticate(any())).thenReturn(Optional.of(principal));

            setupMechanism(List.of(provider1));

            SecurityIdentity result =
                    mechanism
                            .authenticate(routingContext, identityProviderManager)
                            .await()
                            .indefinitely();

            assertNotNull(result);
            assertEquals("testuser", result.getPrincipal().getName());
            verify(auditLogger)
                    .logAccess(
                            eq(AuditEvent.REQUEST_AUTHORIZED),
                            eq(principal),
                            anyString(),
                            eq("ALLOWED"));
            verify(metrics).recordAuthSuccess(anyString());
        }

        @Test
        void shouldIncludeRolesInSecurityIdentity() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-1")
                            .username("admin")
                            .roles(Set.of(Role.ADMIN))
                            .authenticationMethod("OIDC")
                            .build();

            when(provider1.priority()).thenReturn(100);
            when(provider1.supports(any())).thenReturn(true);
            when(provider1.authenticate(any())).thenReturn(Optional.of(principal));

            setupMechanism(List.of(provider1));

            SecurityIdentity result =
                    mechanism
                            .authenticate(routingContext, identityProviderManager)
                            .await()
                            .indefinitely();

            assertNotNull(result);
            assertTrue(result.hasRole("ADMIN"));
        }

        @Test
        void shouldTryProvidersInPriorityOrder() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-1")
                            .username("testuser")
                            .roles(Set.of(Role.VIEWER))
                            .authenticationMethod("API_KEY")
                            .build();

            // provider2 has higher priority
            when(provider1.priority()).thenReturn(50);
            when(provider2.priority()).thenReturn(100);
            when(provider2.supports(any())).thenReturn(true);
            when(provider2.authenticate(any())).thenReturn(Optional.of(principal));

            setupMechanism(List.of(provider1, provider2));

            mechanism.authenticate(routingContext, identityProviderManager).await().indefinitely();

            // provider2 should be tried first (higher priority)
            verify(provider2).supports(any());
            verify(provider2).authenticate(any());
            // provider1 should not be tried since provider2 succeeded
            verify(provider1, never()).authenticate(any());
        }

        @Test
        void shouldSkipProvidersThatDontSupportRequest() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-1")
                            .username("testuser")
                            .roles(Set.of(Role.VIEWER))
                            .authenticationMethod("API_KEY")
                            .build();

            when(provider1.priority()).thenReturn(100);
            when(provider1.supports(any())).thenReturn(false);
            when(provider2.priority()).thenReturn(50);
            when(provider2.supports(any())).thenReturn(true);
            when(provider2.authenticate(any())).thenReturn(Optional.of(principal));

            setupMechanism(List.of(provider1, provider2));

            SecurityIdentity result =
                    mechanism
                            .authenticate(routingContext, identityProviderManager)
                            .await()
                            .indefinitely();

            assertNotNull(result);
            verify(provider1).supports(any());
            verify(provider1, never()).authenticate(any());
            verify(provider2).authenticate(any());
        }
    }

    @Nested
    class FailedAuthentication {

        @BeforeEach
        void setUp() {
            setupAuthConfig(true);
            setupRoutingContext();
        }

        @Test
        void shouldReturnNullWhenNoProviderCanAuthenticate() {
            when(provider1.priority()).thenReturn(100);
            when(provider1.supports(any())).thenReturn(true);
            lenient().when(provider1.authenticate(any())).thenReturn(Optional.empty());

            setupMechanism(List.of(provider1));

            SecurityIdentity result =
                    mechanism
                            .authenticate(routingContext, identityProviderManager)
                            .await()
                            .indefinitely();

            assertNull(result);
            verify(metrics).recordAuthFailure("none", "no_valid_credentials");
        }

        @Test
        void shouldContinueToNextProviderOnError() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-1")
                            .username("testuser")
                            .roles(Set.of(Role.VIEWER))
                            .authenticationMethod("API_KEY")
                            .build();

            when(provider1.priority()).thenReturn(100);
            when(provider1.supports(any())).thenReturn(true);
            when(provider1.authenticate(any())).thenThrow(new RuntimeException("Provider error"));

            when(provider2.priority()).thenReturn(50);
            when(provider2.supports(any())).thenReturn(true);
            when(provider2.authenticate(any())).thenReturn(Optional.of(principal));

            setupMechanism(List.of(provider1, provider2));

            SecurityIdentity result =
                    mechanism
                            .authenticate(routingContext, identityProviderManager)
                            .await()
                            .indefinitely();

            assertNotNull(result);
            verify(auditLogger)
                    .log(eq(AuditEvent.AUTH_ERROR), anyString(), anyString(), anyString());
        }
    }

    @Nested
    class Challenge {

        @Test
        void shouldReturnChallengeWithProviderSchemes() {
            when(provider1.getScheme()).thenReturn("Bearer");
            when(provider2.getScheme()).thenReturn("ApiKey");

            setupMechanism(List.of(provider1, provider2));

            ChallengeData challenge = mechanism.getChallenge(routingContext).await().indefinitely();

            assertEquals(401, challenge.status);
            assertEquals("WWW-Authenticate", challenge.headerName.toString());
            String authHeader = challenge.headerContent;
            assertTrue(authHeader.contains("Bearer") || authHeader.contains("ApiKey"));
        }

        @Test
        void shouldReturnDefaultSchemeWhenNoProviders() {
            setupMechanism(List.of());

            ChallengeData challenge = mechanism.getChallenge(routingContext).await().indefinitely();

            assertEquals(401, challenge.status);
            assertEquals("Bearer, ApiKey", challenge.headerContent);
        }
    }

    @Nested
    class CredentialTypes {

        @Test
        void shouldReturnEmptyCredentialTypes() {
            setupMechanism(List.of());

            Set<?> types = mechanism.getCredentialTypes();

            assertTrue(types.isEmpty());
        }
    }
}
