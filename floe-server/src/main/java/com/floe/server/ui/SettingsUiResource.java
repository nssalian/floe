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

package com.floe.server.ui;

import com.floe.core.catalog.CatalogClient;
import com.floe.core.catalog.CatalogConfig;
import com.floe.core.catalog.CatalogConfigStore;
import com.floe.server.config.FloeConfig;
import com.floe.server.scheduler.SchedulerConfig;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** UI resource for Settings page displaying current configuration. */
@Path("/ui/settings")
@Produces(MediaType.TEXT_HTML)
public class SettingsUiResource extends UiResource {

    private static final Logger LOG = LoggerFactory.getLogger(SettingsUiResource.class);

    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("MMM d, yyyy h:mm a 'UTC'").withZone(ZoneId.of("UTC"));

    @Inject Template settings;

    @Inject CatalogConfigStore catalogConfigStore;

    @Inject CatalogClient catalogClient;

    @Inject FloeConfig floeConfig;

    @Inject SchedulerConfig schedulerConfig;

    @GET
    public TemplateInstance settings() {
        // Lazy init: persist catalog config on first Settings page load if not already persisted
        CatalogConfigView catalogView =
                getOrPersistCatalogConfig()
                        .map(CatalogConfigView::from)
                        .orElse(CatalogConfigView.notConfigured());

        EngineConfigView engineView = EngineConfigView.from(floeConfig);
        StoreConfigView storeView = StoreConfigView.from(floeConfig);
        SchedulerConfigView schedulerView = SchedulerConfigView.from(schedulerConfig);

        return page(settings, "/ui/settings")
                .data("catalog", catalogView)
                .data("catalogHealthy", catalogClient.isHealthy())
                .data("engine", engineView)
                .data("store", storeView)
                .data("scheduler", schedulerView);
    }

    /**
     * Gets existing catalog config from DB, or persists it if catalog is healthy. This is lazy
     * initialization - config is persisted on first Settings page load.
     */
    private Optional<CatalogConfig> getOrPersistCatalogConfig() {
        // Check if already persisted
        Optional<CatalogConfig> existing = catalogConfigStore.findActive();
        if (existing.isPresent()) {
            return existing;
        }

        // Not persisted yet - try to persist if catalog is healthy
        try {
            if (!catalogClient.isHealthy()) {
                return Optional.empty();
            }

            FloeConfig.Catalog catalog = floeConfig.catalog();
            String type = catalog.type().toUpperCase(Locale.ROOT);
            String uri = resolveUri(catalog, type);
            Map<String, String> properties = buildProperties(catalog, type);

            CatalogConfig config =
                    CatalogConfig.create(
                            catalog.name(), type, uri, catalog.warehouse(), properties);

            CatalogConfig saved = catalogConfigStore.upsertAndActivate(config);
            LOG.info("Catalog config persisted: name={}, type={}", saved.name(), saved.type());
            return Optional.of(saved);
        } catch (Exception e) {
            LOG.warn("Failed to persist catalog config: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private String resolveUri(FloeConfig.Catalog catalog, String type) {
        return switch (type) {
            case "REST" -> catalog.uri();
            case "HIVE", "HMS" -> catalog.hive().uri().orElse(null);
            case "NESSIE" -> catalog.nessie().uri().orElse(null);
            case "POLARIS" -> catalog.polaris().uri().orElse(null);
            default -> catalog.uri();
        };
    }

    private Map<String, String> buildProperties(FloeConfig.Catalog catalog, String type) {
        Map<String, String> props = new HashMap<>();
        FloeConfig.Catalog.S3 s3 = catalog.s3();
        s3.endpoint().ifPresent(e -> props.put("s3.endpoint", e));
        props.put("s3.region", s3.region());

        switch (type) {
            case "HIVE", "HMS" ->
                    catalog.hive().configPath().ifPresent(p -> props.put("hive.configPath", p));
            case "NESSIE" -> props.put("nessie.ref", catalog.nessie().ref());
            case "POLARIS" ->
                    catalog.polaris()
                            .principalRole()
                            .ifPresent(r -> props.put("polaris.principalRole", r));
            default -> {}
        }
        return props;
    }

    /** View model for catalog configuration. */
    public record CatalogConfigView(
            String name,
            String type,
            String uri,
            String warehouse,
            Map<String, String> properties,
            String connectedSince,
            boolean configured) {
        public static CatalogConfigView from(CatalogConfig config) {
            return new CatalogConfigView(
                    config.name(),
                    config.type(),
                    config.uri() != null ? config.uri() : "-",
                    config.warehouse(),
                    config.properties(),
                    DATE_FORMAT.format(config.createdAt()),
                    true);
        }

        public static CatalogConfigView notConfigured() {
            return new CatalogConfigView("-", "-", "-", "-", Map.of(), "-", false);
        }
    }

    /** View model for engine configuration. */
    public record EngineConfigView(
            String type, String url, String driverMemory, String executorMemory) {
        public static EngineConfigView from(FloeConfig config) {
            String type = config.engineType();
            if ("SPARK".equalsIgnoreCase(type)) {
                return new EngineConfigView(
                        "SPARK",
                        config.livy().url(),
                        config.livy().driverMemory(),
                        config.livy().executorMemory());
            } else if ("TRINO".equalsIgnoreCase(type)) {
                return new EngineConfigView("TRINO", config.trino().jdbcUrl(), "-", "-");
            }
            return new EngineConfigView(type, "-", "-", "-");
        }
    }

    /** View model for store configuration. */
    public record StoreConfigView(String type) {
        public static StoreConfigView from(FloeConfig config) {
            return new StoreConfigView(config.store().type());
        }
    }

    /** View model for scheduler configuration. */
    public record SchedulerConfigView(
            boolean enabled,
            boolean distributedLock,
            int maxTablesPerPoll,
            int maxOperationsPerPoll,
            String maxBytesPerHourFormatted,
            int failureBackoffThreshold,
            int failureBackoffHours,
            int zeroChangeThreshold,
            int zeroChangeFrequencyReductionPercent,
            int zeroChangeMinIntervalHours) {
        public static SchedulerConfigView from(SchedulerConfig config) {
            return new SchedulerConfigView(
                    config.enabled(),
                    config.distributedLockEnabled(),
                    config.maxTablesPerPoll(),
                    config.maxOperationsPerPoll(),
                    formatBytesOrUnlimited(config.maxBytesPerHour()),
                    config.failureBackoffThreshold(),
                    config.failureBackoffHours(),
                    config.zeroChangeThreshold(),
                    config.zeroChangeFrequencyReductionPercent(),
                    config.zeroChangeMinIntervalHours());
        }
    }

    private static String formatBytesOrUnlimited(long bytes) {
        if (bytes <= 0) {
            return "Unlimited";
        }
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        }
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
