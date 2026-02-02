package com.floe.server.resource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.health.HealthThresholds;
import com.floe.core.policy.MaintenancePolicy;
import com.floe.core.policy.PolicyStore;
import com.floe.core.policy.TablePattern;
import com.floe.server.api.CreatePolicyRequest;
import com.floe.server.api.HealthThresholdsDto;
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
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PoliciesResourceThresholdsTest {

    @Mock PolicyStore policyStore;

    @Mock CatalogClient catalogClient;

    @Mock FloeMetrics metrics;

    @InjectMocks PolicyResource resource;

    @BeforeEach
    void setUp() {
        Mockito.lenient().when(catalogClient.isHealthy()).thenReturn(true);
        Mockito.lenient().when(catalogClient.getCatalogName()).thenReturn("demo");
        Mockito.lenient()
                .when(catalogClient.listAllTables())
                .thenReturn(List.of(TableIdentifier.of("demo", "db", "table")));
        Mockito.lenient().when(policyStore.listEnabled()).thenReturn(List.of());
    }

    @Test
    void createPolicyWithHealthThresholds() {
        when(policyStore.existsByName(anyString())).thenReturn(false);
        CreatePolicyRequest request =
                new CreatePolicyRequest(
                        "policy",
                        "desc",
                        "demo.db.*",
                        10,
                        Map.of(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        sampleThresholds(),
                        null // triggerConditions
                        );

        resource.create(request);

        ArgumentCaptor<MaintenancePolicy> captor = ArgumentCaptor.forClass(MaintenancePolicy.class);
        verify(policyStore).save(captor.capture());

        assertThat(captor.getValue().healthThresholds())
                .extracting(HealthThresholds::deleteFileRatioWarning)
                .isEqualTo(0.12);
    }

    @Test
    void createPolicyWithoutHealthThresholds() {
        when(policyStore.existsByName(anyString())).thenReturn(false);
        CreatePolicyRequest request =
                new CreatePolicyRequest(
                        "policy",
                        "desc",
                        "demo.db.*",
                        10,
                        Map.of(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null, // healthThresholds
                        null // triggerConditions
                        );

        resource.create(request);

        ArgumentCaptor<MaintenancePolicy> captor = ArgumentCaptor.forClass(MaintenancePolicy.class);
        verify(policyStore).save(captor.capture());

        assertThat(captor.getValue().healthThresholds()).isNull();
    }

    @Test
    void updatePolicyCanSetThresholds() {
        String id = UUID.randomUUID().toString();
        MaintenancePolicy existing =
                new MaintenancePolicy(
                        id,
                        "policy",
                        "desc",
                        TablePattern.parse("demo.db.*"),
                        true,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        10,
                        null, // healthThresholds
                        null, // triggerConditions
                        Map.of(),
                        Instant.now(),
                        Instant.now());
        when(policyStore.getById(id)).thenReturn(Optional.of(existing));

        UpdatePolicyRequest request =
                new UpdatePolicyRequest(
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        sampleThresholds(),
                        null // triggerConditions
                        );

        resource.update(id, request);

        ArgumentCaptor<MaintenancePolicy> captor = ArgumentCaptor.forClass(MaintenancePolicy.class);
        verify(policyStore).save(captor.capture());

        assertThat(captor.getValue().healthThresholds())
                .extracting(HealthThresholds::partitionSkewCritical)
                .isEqualTo(12.0);
    }

    @Test
    void getPolicyIncludesThresholds() {
        MaintenancePolicy policy =
                MaintenancePolicy.builder()
                        .name("policy")
                        .tablePattern(TablePattern.parse("demo.db.*"))
                        .healthThresholds(sampleThresholds().toThresholds())
                        .build();
        when(policyStore.getById(policy.id())).thenReturn(Optional.of(policy));

        Response response = resource.getById(policy.id());

        PolicyResponse body = (PolicyResponse) response.getEntity();
        assertThat(body.healthThresholds()).isNotNull();
        assertThat(body.healthThresholds().partitionSkewWarning()).isEqualTo(4.0);
    }

    private HealthThresholdsDto sampleThresholds() {
        return new HealthThresholdsDto(
                null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                0.12, 0.30, null, null, null, null, null, null, 4.0, 12.0, null, null);
    }
}
