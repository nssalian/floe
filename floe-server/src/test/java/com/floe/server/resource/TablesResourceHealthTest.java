package com.floe.server.resource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.TableIdentifier;
import com.floe.core.health.HealthReport;
import com.floe.core.health.TableHealthStore;
import com.floe.core.policy.PolicyMatcher;
import com.floe.server.api.HealthReportResponse;
import com.floe.server.config.HealthConfig;
import com.floe.server.health.HealthReportCache;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TablesResourceHealthTest {

    @Mock CatalogClient catalogClient;

    @Mock HealthConfig healthConfig;

    @Mock TableHealthStore healthStore;

    @Mock PolicyMatcher policyMatcher;

    @Mock HealthReportCache healthReportCache;

    @InjectMocks TablesResource resource;

    @BeforeEach
    void setUp() {
        lenient().when(healthConfig.scanMode()).thenReturn("scan");
        lenient().when(healthConfig.sampleLimit()).thenReturn(1000);
        lenient().when(healthConfig.persistenceEnabled()).thenReturn(true);
        lenient().when(healthConfig.maxReportsPerTable()).thenReturn(100);
        lenient().when(catalogClient.getCatalogName()).thenReturn("demo");
        lenient()
                .when(healthStore.findHistory(anyString(), anyString(), anyString(), anyInt()))
                .thenReturn(List.of());
        lenient()
                .when(policyMatcher.findEffectivePolicy(anyString(), any(TableIdentifier.class)))
                .thenReturn(Optional.empty());
        lenient().when(healthReportCache.getIfFresh(any())).thenReturn(Optional.empty());
    }

    @Test
    void getHealthIncludesThresholds() {
        Table mockTable = mock(Table.class, withSettings().lenient());
        when(mockTable.currentSnapshot()).thenReturn(null);
        when(mockTable.snapshots()).thenReturn(List.of());
        when(catalogClient.loadTable(any(TableIdentifier.class)))
                .thenReturn(Optional.of(mockTable));

        Response response = resource.getTableHealth("test", "events");

        assertThat(response.getStatus()).isEqualTo(200);
        HealthReportResponse body = (HealthReportResponse) response.getEntity();
        assertThat(body.thresholds()).isNotNull();
    }

    @Test
    void getHealthPersistsReportAndPrunes() {
        Table mockTable = mock(Table.class, withSettings().lenient());
        when(mockTable.currentSnapshot()).thenReturn(null);
        when(mockTable.snapshots()).thenReturn(List.of());
        when(catalogClient.loadTable(any(TableIdentifier.class)))
                .thenReturn(Optional.of(mockTable));

        Response response = resource.getTableHealth("test", "events");

        assertThat(response.getStatus()).isEqualTo(200);
        verify(healthStore).save(any(HealthReport.class));
        verify(healthStore).pruneHistory(eq("demo"), eq("test"), eq("events"), eq(100));
    }

    @Test
    void getHealthHistoryReturnsHistory() {
        when(healthStore.findHistory("demo", "test", "events", 10)).thenReturn(List.of());
        when(catalogClient.loadTable(any(TableIdentifier.class)))
                .thenReturn(Optional.of(mock(Table.class, withSettings().lenient())));

        Response response = resource.getTableHealthHistory("test", "events", 10);

        assertThat(response.getStatus()).isEqualTo(200);
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertThat(body.get("catalog")).isEqualTo("demo");
        assertThat(body.get("history")).isNotNull();
    }

    @Test
    void getLatestHealthReportsReturnsAcrossTables() {
        when(healthStore.findLatest(20)).thenReturn(List.of());

        Response response = resource.getLatestHealthReports(20);

        assertThat(response.getStatus()).isEqualTo(200);
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertThat(body.get("catalog")).isEqualTo("demo");
        assertThat(body.get("reports")).isNotNull();
    }
}
