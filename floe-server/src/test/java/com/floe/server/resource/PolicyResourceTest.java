package com.floe.server.resource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.PolicyStore;
import com.floe.core.policy.TablePattern;
import com.floe.server.api.CreatePolicyRequest;
import com.floe.server.api.ErrorResponse;
import com.floe.server.api.PolicyResponse;
import com.floe.server.api.UpdatePolicyRequest;
import com.floe.server.metrics.FloeMetrics;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PolicyResourceTest {

    @Mock PolicyStore policyStore;

    @Mock CatalogClient catalogClient;

    @Mock FloeMetrics metrics;

    @InjectMocks PolicyResource resource;

    private MaintenancePolicy testPolicy;

    @BeforeEach
    void setUp() {
        Instant now = Instant.now();
        testPolicy =
                new MaintenancePolicy(
                        UUID.randomUUID().toString(),
                        "test-policy",
                        "Test policy",
                        TablePattern.parse("demo.test.*"),
                        true,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        50,
                        null, // healthThresholds
                        null, // triggerConditions
                        Map.of(),
                        now,
                        now);
    }

    // List tests

    @Test
    void shouldListAllPolicies() {
        when(policyStore.listAll()).thenReturn(List.of(testPolicy));

        Response response = resource.list(null, 20, 0);

        assertEquals(200, response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals(1, body.get("total"));

        @SuppressWarnings("unchecked")
        List<?> policies = (List<?>) body.get("policies");
        assertEquals(1, policies.size());
    }

    @Test
    void shouldListEnabledPoliciesOnly() {
        when(policyStore.listEnabled()).thenReturn(List.of(testPolicy));

        Response response = resource.list(true, 20, 0);

        assertEquals(200, response.getStatus());
        verify(policyStore).listEnabled();
        verify(policyStore, never()).listAll();
    }

    @Test
    void shouldReturnEmptyListWhenNoPolicies() {
        when(policyStore.listAll()).thenReturn(List.of());

        Response response = resource.list(null, 20, 0);

        assertEquals(200, response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals(0, body.get("total"));
    }

    // Get by ID tests

    @Test
    void shouldGetPolicyById() {
        when(policyStore.getById(testPolicy.id())).thenReturn(Optional.of(testPolicy));

        Response response = resource.getById(testPolicy.id());

        assertEquals(200, response.getStatus());
        PolicyResponse body = (PolicyResponse) response.getEntity();
        assertEquals(testPolicy.name(), body.name());
    }

    @Test
    void shouldReturn404WhenPolicyNotFound() {
        when(policyStore.getById("unknown-id")).thenReturn(Optional.empty());

        Response response = resource.getById("unknown-id");

        assertEquals(404, response.getStatus());
        ErrorResponse body = (ErrorResponse) response.getEntity();
        assertTrue(body.error().contains("not found"));
    }

    // Delete tests

    @Test
    void shouldDeletePolicy() {
        when(policyStore.existsById(testPolicy.id())).thenReturn(true);
        when(policyStore.deleteById(testPolicy.id())).thenReturn(true);

        Response response = resource.delete(testPolicy.id());

        assertEquals(204, response.getStatus());
        verify(policyStore).deleteById(testPolicy.id());
    }

    @Test
    void shouldReturn404WhenDeletingNonExistentPolicy() {
        when(policyStore.existsById("unknown-id")).thenReturn(false);

        Response response = resource.delete("unknown-id");

        assertEquals(404, response.getStatus());
    }

    // Create tests

    @Test
    void shouldReturn409WhenCreatingDuplicateName() {
        CreatePolicyRequest request = mock(CreatePolicyRequest.class);
        when(request.name()).thenReturn("existing-policy");
        when(policyStore.existsByName("existing-policy")).thenReturn(true);

        Response response = resource.create(request);

        assertEquals(409, response.getStatus());
        ErrorResponse body = (ErrorResponse) response.getEntity();
        assertTrue(body.error().contains("already exists"));
    }

    @Test
    void shouldRejectPolicyWhenCatalogUnavailable() {
        CreatePolicyRequest request = mock(CreatePolicyRequest.class);
        when(request.name()).thenReturn("new-policy");
        when(request.toPolicy()).thenReturn(testPolicy);
        when(policyStore.existsByName("new-policy")).thenReturn(false);
        when(catalogClient.isHealthy()).thenReturn(false);

        Response response = resource.create(request);

        assertEquals(400, response.getStatus());
        ErrorResponse body = (ErrorResponse) response.getEntity();
        assertTrue(body.error().contains("Catalog unavailable"));
        verify(policyStore, never()).save(any());
    }

    @Test
    void shouldCreatePolicyWithValidPattern() {
        CreatePolicyRequest request = mock(CreatePolicyRequest.class);
        when(request.name()).thenReturn("new-policy");
        when(request.toPolicy()).thenReturn(testPolicy);
        when(policyStore.existsByName("new-policy")).thenReturn(false);
        when(catalogClient.isHealthy()).thenReturn(true);
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.listAllTables())
                .thenReturn(List.of(TableIdentifier.of("demo", "test", "events")));

        Response response = resource.create(request);

        assertEquals(201, response.getStatus());
        verify(policyStore).save(testPolicy);
    }

    @Test
    void shouldRejectExactPatternWithNoMatches() {
        MaintenancePolicy exactPolicy =
                new MaintenancePolicy(
                        UUID.randomUUID().toString(),
                        "exact-policy",
                        "Exact pattern policy",
                        TablePattern.parse("demo.test.nonexistent"), // Exact pattern, no wildcards
                        true,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        50,
                        null, // healthThresholds
                        null, // triggerConditions
                        Map.of(),
                        Instant.now(),
                        Instant.now());

        CreatePolicyRequest request = mock(CreatePolicyRequest.class);
        when(request.name()).thenReturn("exact-policy");
        when(request.toPolicy()).thenReturn(exactPolicy);
        when(policyStore.existsByName("exact-policy")).thenReturn(false);
        when(catalogClient.isHealthy()).thenReturn(true);
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.listAllTables())
                .thenReturn(List.of(TableIdentifier.of("demo", "test", "events")));

        Response response = resource.create(request);

        assertEquals(400, response.getStatus());
        ErrorResponse body = (ErrorResponse) response.getEntity();
        assertTrue(body.error().contains("not found"));
    }

    @Test
    void shouldAllowWildcardPatternWithNoMatches() {
        MaintenancePolicy wildcardPolicy =
                new MaintenancePolicy(
                        UUID.randomUUID().toString(),
                        "wildcard-policy",
                        "Wildcard pattern policy",
                        TablePattern.parse("demo.future.*"), // Wildcard pattern
                        true,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        50,
                        null, // healthThresholds
                        null, // triggerConditions
                        Map.of(),
                        Instant.now(),
                        Instant.now());

        CreatePolicyRequest request = mock(CreatePolicyRequest.class);
        when(request.name()).thenReturn("wildcard-policy");
        when(request.toPolicy()).thenReturn(wildcardPolicy);
        when(policyStore.existsByName("wildcard-policy")).thenReturn(false);
        when(catalogClient.isHealthy()).thenReturn(true);
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.listAllTables())
                .thenReturn(List.of(TableIdentifier.of("demo", "test", "events")));

        Response response = resource.create(request);

        // Wildcard with no matches = warning but allowed
        assertEquals(201, response.getStatus());
        verify(policyStore).save(wildcardPolicy);
    }

    // Update tests

    @Test
    void shouldReturn404WhenUpdatingNonExistentPolicy() {
        UpdatePolicyRequest request = mock(UpdatePolicyRequest.class);
        when(policyStore.getById("unknown-id")).thenReturn(Optional.empty());

        Response response = resource.update("unknown-id", request);

        assertEquals(404, response.getStatus());
    }

    @Test
    void shouldReturn409WhenUpdatingToExistingName() {
        UpdatePolicyRequest request = mock(UpdatePolicyRequest.class);
        when(request.name()).thenReturn("existing-name");
        when(policyStore.getById(testPolicy.id())).thenReturn(Optional.of(testPolicy));
        when(policyStore.existsByName("existing-name")).thenReturn(true);

        Response response = resource.update(testPolicy.id(), request);

        assertEquals(409, response.getStatus());
    }

    @Test
    void shouldUpdatePolicy() {
        MaintenancePolicy updatedPolicy =
                new MaintenancePolicy(
                        testPolicy.id(),
                        "updated-name",
                        "Updated description",
                        testPolicy.tablePattern(),
                        true,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        50,
                        null, // healthThresholds
                        null, // triggerConditions
                        Map.of(),
                        testPolicy.createdAt(),
                        Instant.now());

        UpdatePolicyRequest request = mock(UpdatePolicyRequest.class);
        when(request.name()).thenReturn("updated-name");
        when(request.tablePattern()).thenReturn(null); // Pattern not changed
        when(request.applyTo(testPolicy)).thenReturn(updatedPolicy);
        when(policyStore.getById(testPolicy.id())).thenReturn(Optional.of(testPolicy));
        when(policyStore.existsByName("updated-name")).thenReturn(false);

        Response response = resource.update(testPolicy.id(), request);

        assertEquals(200, response.getStatus());
        verify(policyStore).save(updatedPolicy);
    }

    // Validate pattern tests

    @Test
    void shouldValidatePatternEndpoint() {
        when(catalogClient.isHealthy()).thenReturn(true);
        when(catalogClient.getCatalogName()).thenReturn("demo");
        when(catalogClient.listAllTables())
                .thenReturn(
                        List.of(
                                TableIdentifier.of("demo", "test", "events"),
                                TableIdentifier.of("demo", "test", "users")));

        Response response = resource.validatePattern(Map.of("pattern", "demo.test.*"));

        assertEquals(200, response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals(true, body.get("valid"));
        assertEquals(2, body.get("matchCount"));
    }

    @Test
    void shouldRejectEmptyPattern() {
        Response response = resource.validatePattern(Map.of("pattern", ""));

        assertEquals(400, response.getStatus());
    }

    @Test
    void shouldRejectMissingPattern() {
        Response response = resource.validatePattern(Map.of());

        assertEquals(400, response.getStatus());
    }
}
