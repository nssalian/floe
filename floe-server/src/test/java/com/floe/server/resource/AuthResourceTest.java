package com.floe.server.resource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.floe.core.auth.*;
import com.floe.server.api.ApiKeyRequest;
import com.floe.server.api.ApiKeyResponse;
import com.floe.server.api.CreateApiKeyResponse;
import com.floe.server.api.ErrorResponse;
import com.floe.server.api.UpdateApiKeyRequest;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@DisplayName("AuthResource")
class AuthResourceTest {

    @Mock private ApiKeyStore apiKeyStore;

    @Mock private SecurityContext securityContext;

    private AuthResource resource;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        resource = new AuthResource();
        resource.apiKeyStore = apiKeyStore;
        resource.securityContext = securityContext;
    }

    @Nested
    @DisplayName("POST /api/v1/auth/keys (create)")
    class Create {

        @Test
        @DisplayName("should create API key successfully")
        void shouldCreateApiKeySuccessfully() {
            ApiKeyRequest request = new ApiKeyRequest("my-key", Role.OPERATOR, null);
            when(apiKeyStore.existsByName("my-key")).thenReturn(false);

            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("admin-123")
                            .username("admin")
                            .roles(java.util.Set.of(Role.ADMIN))
                            .authenticationMethod("API_KEY")
                            .build();
            when(securityContext.getUserPrincipal()).thenReturn(principal);

            Response response = resource.create(request);

            assertThat(response.getStatus()).isEqualTo(201);
            assertThat(response.getLocation().getPath()).contains("/api/v1/auth/keys/");

            CreateApiKeyResponse body = (CreateApiKeyResponse) response.getEntity();
            assertThat(body.key()).startsWith("floe_");
            assertThat(body.apiKey().name()).isEqualTo("my-key");
            assertThat(body.apiKey().role()).isEqualTo(Role.OPERATOR);

            ArgumentCaptor<ApiKey> captor = ArgumentCaptor.forClass(ApiKey.class);
            verify(apiKeyStore).save(captor.capture());
            assertThat(captor.getValue().name()).isEqualTo("my-key");
            assertThat(captor.getValue().createdBy()).isEqualTo("admin-123");
        }

        @Test
        @DisplayName("should return 409 when name already exists")
        void shouldReturn409WhenNameExists() {
            ApiKeyRequest request = new ApiKeyRequest("existing-key", Role.VIEWER, null);
            when(apiKeyStore.existsByName("existing-key")).thenReturn(true);

            Response response = resource.create(request);

            assertThat(response.getStatus()).isEqualTo(409);
            ErrorResponse body = (ErrorResponse) response.getEntity();
            assertThat(body.error()).contains("already exists");
            verify(apiKeyStore, never()).save(any());
        }

        @Test
        @DisplayName("should create key with expiration")
        void shouldCreateKeyWithExpiration() {
            Instant expiresAt = Instant.now().plus(30, ChronoUnit.DAYS);
            ApiKeyRequest request = new ApiKeyRequest("expiring-key", Role.VIEWER, expiresAt);
            when(apiKeyStore.existsByName("expiring-key")).thenReturn(false);

            Response response = resource.create(request);

            assertThat(response.getStatus()).isEqualTo(201);

            ArgumentCaptor<ApiKey> captor = ArgumentCaptor.forClass(ApiKey.class);
            verify(apiKeyStore).save(captor.capture());
            assertThat(captor.getValue().expiresAt()).isEqualTo(expiresAt);
        }

        @Test
        @DisplayName("should default to VIEWER role when not specified")
        void shouldDefaultToViewerRole() {
            ApiKeyRequest request = new ApiKeyRequest("default-role-key", null, null);
            when(apiKeyStore.existsByName("default-role-key")).thenReturn(false);

            Response response = resource.create(request);

            assertThat(response.getStatus()).isEqualTo(201);

            ArgumentCaptor<ApiKey> captor = ArgumentCaptor.forClass(ApiKey.class);
            verify(apiKeyStore).save(captor.capture());
            assertThat(captor.getValue().role()).isEqualTo(Role.VIEWER);
        }
    }

    @Nested
    @DisplayName("GET /api/v1/auth/keys (list)")
    class ListKeys {

        @Test
        @DisplayName("should list all API keys")
        void shouldListAllApiKeys() {
            ApiKey key1 = createApiKey("key-1", "Key One", Role.ADMIN);
            ApiKey key2 = createApiKey("key-2", "Key Two", Role.OPERATOR);
            when(apiKeyStore.listAll()).thenReturn(List.of(key1, key2));

            Response response = resource.list(null);

            assertThat(response.getStatus()).isEqualTo(200);
            @SuppressWarnings("unchecked")
            Map<String, Object> body = (Map<String, Object>) response.getEntity();
            assertThat(body.get("total")).isEqualTo(2);
            @SuppressWarnings("unchecked")
            List<ApiKeyResponse> keys = (List<ApiKeyResponse>) body.get("keys");
            assertThat(keys).hasSize(2);
        }

        @Test
        @DisplayName("should list only enabled keys when filter applied")
        void shouldListOnlyEnabledKeys() {
            ApiKey enabledKey = createApiKey("key-1", "Enabled Key", Role.VIEWER);
            when(apiKeyStore.listEnabled()).thenReturn(List.of(enabledKey));

            Response response = resource.list(true);

            assertThat(response.getStatus()).isEqualTo(200);
            verify(apiKeyStore).listEnabled();
            verify(apiKeyStore, never()).listAll();
        }

        @Test
        @DisplayName("should return empty list when no keys")
        void shouldReturnEmptyList() {
            when(apiKeyStore.listAll()).thenReturn(List.of());

            Response response = resource.list(null);

            assertThat(response.getStatus()).isEqualTo(200);
            @SuppressWarnings("unchecked")
            Map<String, Object> body = (Map<String, Object>) response.getEntity();
            assertThat(body.get("total")).isEqualTo(0);
        }
    }

    @Nested
    @DisplayName("GET /api/v1/auth/keys/{id} (getById)")
    class GetById {

        @Test
        @DisplayName("should return API key when found")
        void shouldReturnApiKeyWhenFound() {
            ApiKey key = createApiKey("key-123", "My Key", Role.OPERATOR);
            when(apiKeyStore.findById("key-123")).thenReturn(Optional.of(key));

            Response response = resource.getById("key-123");

            assertThat(response.getStatus()).isEqualTo(200);
            ApiKeyResponse body = (ApiKeyResponse) response.getEntity();
            assertThat(body.id()).isEqualTo("key-123");
            assertThat(body.name()).isEqualTo("My Key");
        }

        @Test
        @DisplayName("should return 404 when key not found")
        void shouldReturn404WhenNotFound() {
            when(apiKeyStore.findById("non-existent")).thenReturn(Optional.empty());

            Response response = resource.getById("non-existent");

            assertThat(response.getStatus()).isEqualTo(404);
        }
    }

    @Nested
    @DisplayName("PUT /api/v1/auth/keys/{id} (update)")
    class Update {

        @Test
        @DisplayName("should update API key name")
        void shouldUpdateApiKeyName() {
            ApiKey existing = createApiKey("key-123", "Old Name", Role.VIEWER);
            when(apiKeyStore.findById("key-123")).thenReturn(Optional.of(existing));
            when(apiKeyStore.existsByName("New Name")).thenReturn(false);

            UpdateApiKeyRequest request = new UpdateApiKeyRequest("New Name", null, null);

            Response response = resource.update("key-123", request);

            assertThat(response.getStatus()).isEqualTo(200);
            ArgumentCaptor<ApiKey> captor = ArgumentCaptor.forClass(ApiKey.class);
            verify(apiKeyStore).save(captor.capture());
            assertThat(captor.getValue().name()).isEqualTo("New Name");
        }

        @Test
        @DisplayName("should update API key role")
        void shouldUpdateApiKeyRole() {
            ApiKey existing = createApiKey("key-123", "My Key", Role.VIEWER);
            when(apiKeyStore.findById("key-123")).thenReturn(Optional.of(existing));

            UpdateApiKeyRequest request = new UpdateApiKeyRequest(null, Role.ADMIN, null);

            Response response = resource.update("key-123", request);

            assertThat(response.getStatus()).isEqualTo(200);
            ArgumentCaptor<ApiKey> captor = ArgumentCaptor.forClass(ApiKey.class);
            verify(apiKeyStore).save(captor.capture());
            assertThat(captor.getValue().role()).isEqualTo(Role.ADMIN);
        }

        @Test
        @DisplayName("should disable API key")
        void shouldDisableApiKey() {
            ApiKey existing = createApiKey("key-123", "My Key", Role.OPERATOR);
            when(apiKeyStore.findById("key-123")).thenReturn(Optional.of(existing));

            UpdateApiKeyRequest request = new UpdateApiKeyRequest(null, null, false);

            Response response = resource.update("key-123", request);

            assertThat(response.getStatus()).isEqualTo(200);
            ArgumentCaptor<ApiKey> captor = ArgumentCaptor.forClass(ApiKey.class);
            verify(apiKeyStore).save(captor.capture());
            assertThat(captor.getValue().enabled()).isFalse();
        }

        @Test
        @DisplayName("should return 404 when key not found")
        void shouldReturn404WhenNotFound() {
            when(apiKeyStore.findById("non-existent")).thenReturn(Optional.empty());

            UpdateApiKeyRequest request = new UpdateApiKeyRequest("New Name", null, null);

            Response response = resource.update("non-existent", request);

            assertThat(response.getStatus()).isEqualTo(404);
        }

        @Test
        @DisplayName("should return 409 when new name already exists")
        void shouldReturn409WhenNewNameExists() {
            ApiKey existing = createApiKey("key-123", "Old Name", Role.VIEWER);
            when(apiKeyStore.findById("key-123")).thenReturn(Optional.of(existing));
            when(apiKeyStore.existsByName("Taken Name")).thenReturn(true);

            UpdateApiKeyRequest request = new UpdateApiKeyRequest("Taken Name", null, null);

            Response response = resource.update("key-123", request);

            assertThat(response.getStatus()).isEqualTo(409);
            verify(apiKeyStore, never()).save(any());
        }

        @Test
        @DisplayName("should allow keeping same name")
        void shouldAllowKeepingSameName() {
            ApiKey existing = createApiKey("key-123", "Same Name", Role.VIEWER);
            when(apiKeyStore.findById("key-123")).thenReturn(Optional.of(existing));

            UpdateApiKeyRequest request = new UpdateApiKeyRequest("Same Name", Role.OPERATOR, null);

            Response response = resource.update("key-123", request);

            assertThat(response.getStatus()).isEqualTo(200);
            verify(apiKeyStore, never()).existsByName(anyString());
        }
    }

    @Nested
    @DisplayName("DELETE /api/v1/auth/keys/{id} (delete)")
    class Delete {

        @Test
        @DisplayName("should delete API key successfully")
        void shouldDeleteApiKeySuccessfully() {
            when(apiKeyStore.existsById("key-123")).thenReturn(true);
            when(apiKeyStore.deleteById("key-123")).thenReturn(true);

            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("admin-456")
                            .username("admin")
                            .roles(java.util.Set.of(Role.ADMIN))
                            .authenticationMethod("API_KEY")
                            .build();
            when(securityContext.getUserPrincipal()).thenReturn(principal);

            Response response = resource.delete("key-123");

            assertThat(response.getStatus()).isEqualTo(204);
            verify(apiKeyStore).deleteById("key-123");
        }

        @Test
        @DisplayName("should return 404 when key not found")
        void shouldReturn404WhenNotFound() {
            when(apiKeyStore.existsById("non-existent")).thenReturn(false);

            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("admin-456")
                            .username("admin")
                            .roles(java.util.Set.of(Role.ADMIN))
                            .authenticationMethod("API_KEY")
                            .build();
            when(securityContext.getUserPrincipal()).thenReturn(principal);

            Response response = resource.delete("non-existent");

            assertThat(response.getStatus()).isEqualTo(404);
        }

        @Test
        @DisplayName("should prevent deleting own API key")
        void shouldPreventDeletingOwnApiKey() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("key-123")
                            .username("self")
                            .roles(java.util.Set.of(Role.ADMIN))
                            .authenticationMethod("API_KEY")
                            .build();
            when(securityContext.getUserPrincipal()).thenReturn(principal);

            Response response = resource.delete("key-123");

            assertThat(response.getStatus()).isEqualTo(400);
            ErrorResponse body = (ErrorResponse) response.getEntity();
            assertThat(body.error()).contains("Cannot delete your own");
            verify(apiKeyStore, never()).deleteById(anyString());
        }
    }

    @Nested
    @DisplayName("GET /api/v1/auth/keys/me")
    class Me {

        @Test
        @DisplayName("should return API key info for API key authentication")
        void shouldReturnApiKeyInfoForApiKeyAuth() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("key-123")
                            .username("My API Key")
                            .roles(java.util.Set.of(Role.OPERATOR))
                            .authenticationMethod("API_KEY")
                            .build();
            when(securityContext.getUserPrincipal()).thenReturn(principal);

            ApiKey apiKey = createApiKey("key-123", "My API Key", Role.OPERATOR);
            when(apiKeyStore.findById("key-123")).thenReturn(Optional.of(apiKey));

            Response response = resource.me();

            assertThat(response.getStatus()).isEqualTo(200);
            ApiKeyResponse body = (ApiKeyResponse) response.getEntity();
            assertThat(body.id()).isEqualTo("key-123");
            assertThat(body.name()).isEqualTo("My API Key");
        }

        @Test
        @DisplayName("should return principal info for OIDC authentication")
        void shouldReturnPrincipalInfoForOidcAuth() {
            FloePrincipal principal =
                    FloePrincipal.builder()
                            .userId("user-456")
                            .username("john.doe")
                            .roles(java.util.Set.of(Role.ADMIN))
                            .authenticationMethod("OIDC")
                            .build();
            when(securityContext.getUserPrincipal()).thenReturn(principal);

            Response response = resource.me();

            assertThat(response.getStatus()).isEqualTo(200);
            @SuppressWarnings("unchecked")
            Map<String, Object> body = (Map<String, Object>) response.getEntity();
            assertThat(body.get("userId")).isEqualTo("user-456");
            assertThat(body.get("username")).isEqualTo("john.doe");
            assertThat(body.get("authMethod")).isEqualTo("OIDC");
        }

        @Test
        @DisplayName("should return 401 when not authenticated")
        void shouldReturn401WhenNotAuthenticated() {
            when(securityContext.getUserPrincipal()).thenReturn(null);

            Response response = resource.me();

            assertThat(response.getStatus()).isEqualTo(401);
        }
    }

    // Helper method to create test API keys
    private ApiKey createApiKey(String id, String name, Role role) {
        return ApiKey.builder()
                .id(id)
                .keyHash(ApiKeyGenerator.hashKey("floe_test12345678901234567890ab"))
                .name(name)
                .role(role)
                .enabled(true)
                .createdBy("test-admin")
                .build();
    }
}
