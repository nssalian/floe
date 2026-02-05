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

package com.floe.server.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.smallrye.config.common.MapBackedConfigSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.jboss.logging.Logger;

/**
 * Custom ConfigSource that reads floe-config.yaml and maps it to Quarkus properties.
 *
 * <p>This allows users to provide a clean, Floe-specific YAML configuration file that gets
 * translated to internal Quarkus properties at startup.
 *
 * <p>Config file locations (in order of precedence):
 *
 * <ol>
 *   <li>Path specified by FLOE_CONFIG_PATH environment variable
 *   <li>/config/floe-config.yaml (container mount point)
 *   <li>./floe-config.yaml (current working directory)
 * </ol>
 *
 * <p>Priority is set to 275 (higher than application.properties at 250, but lower than env vars at
 * 300).
 */
public class FloeConfigSource extends MapBackedConfigSource {

    private static final Logger LOG = Logger.getLogger(FloeConfigSource.class);
    private static final String NAME = "FloeConfigSource";

    // Priority: higher than application.properties (250), lower than env vars (300)
    private static final int ORDINAL = 275;

    private static final String[] CONFIG_LOCATIONS = {
        System.getenv("FLOE_CONFIG_PATH"),
        "/config/floe-config.yaml",
        "./floe-config.yaml",
        "floe-config.yaml",
    };

    public FloeConfigSource() {
        super(NAME, loadConfig(), ORDINAL);
    }

    private static Map<String, String> loadConfig() {
        Map<String, String> properties = new HashMap<>();

        Path configPath = findConfigFile();
        if (configPath == null) {
            LOG.debug("No floe-config.yaml found, using default configuration");
            return properties;
        }

        LOG.infof("Loading Floe configuration from: %s", configPath);

        try {
            String yaml = Files.readString(configPath);
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            JsonNode root = mapper.readTree(yaml);

            // Map floe-config.yaml structure to Quarkus properties
            mapCatalogConfig(root.get("catalog"), properties);
            mapEngineConfig(root.get("engine"), properties);
            mapStoreConfig(root.get("store"), properties);
            mapSchedulerConfig(root.get("scheduler"), properties);
            mapAuthConfig(root.get("auth"), properties);
            mapOidcConfig(root.get("oidc"), properties);
            mapSecurityConfig(root.get("security"), properties);
            mapEventsConfig(root.get("events"), properties);
            mapServerConfig(root.get("server"), properties);
            mapLoggingConfig(root.get("logging"), properties);

            LOG.infof(
                    "Loaded %d configuration properties from floe-config.yaml", properties.size());
        } catch (IOException e) {
            LOG.errorf(e, "Failed to load floe-config.yaml from %s", configPath);
        }

        return properties;
    }

    private static Path findConfigFile() {
        for (String location : CONFIG_LOCATIONS) {
            if (location == null || location.isBlank()) {
                continue;
            }
            Path path = Path.of(location);
            if (Files.exists(path) && Files.isReadable(path)) {
                return path;
            }
        }
        return null;
    }

    // Catalog Configuration Mapping

    private static void mapCatalogConfig(JsonNode catalog, Map<String, String> props) {
        if (catalog == null) return;

        setIfPresent(props, "floe.catalog.type", catalog.get("type"));
        setIfPresent(props, "floe.catalog.name", catalog.get("name"));
        setIfPresent(props, "floe.catalog.warehouse", catalog.get("warehouse"));
        setIfPresent(props, "floe.catalog.uri", catalog.get("uri"));

        // S3 configuration
        JsonNode s3 = catalog.get("s3");
        if (s3 != null) {
            setIfPresent(props, "floe.catalog.s3.endpoint", s3.get("endpoint"));
            setIfPresent(props, "floe.catalog.s3.region", s3.get("region"));
            setIfPresent(props, "floe.catalog.s3.access-key-id", s3.get("accessKeyId"));
            setIfPresent(props, "floe.catalog.s3.secret-access-key", s3.get("secretAccessKey"));
            // pathStyleAccess is a boolean but we store as string
            if (s3.has("pathStyleAccess")) {
                props.put("floe.catalog.s3.path-style-access", s3.get("pathStyleAccess").asText());
            }
        }

        // Hive configuration
        JsonNode hive = catalog.get("hive");
        if (hive != null) {
            setIfPresent(props, "floe.catalog.hive.uri", hive.get("uri"));
            setIfPresent(props, "floe.catalog.hive.config-path", hive.get("configPath"));
        }

        // Nessie configuration
        JsonNode nessie = catalog.get("nessie");
        if (nessie != null) {
            setIfPresent(props, "floe.catalog.nessie.uri", nessie.get("uri"));
            setIfPresent(props, "floe.catalog.nessie.ref", nessie.get("ref"));
            setIfPresent(props, "floe.catalog.nessie.token", nessie.get("token"));
        }

        // Polaris configuration
        JsonNode polaris = catalog.get("polaris");
        if (polaris != null) {
            setIfPresent(props, "floe.catalog.polaris.uri", polaris.get("uri"));
            setIfPresent(props, "floe.catalog.polaris.client-id", polaris.get("clientId"));
            setIfPresent(props, "floe.catalog.polaris.client-secret", polaris.get("clientSecret"));
            setIfPresent(
                    props, "floe.catalog.polaris.principal-role", polaris.get("principalRole"));
        }
    }

    // Engine Configuration Mapping

    private static void mapEngineConfig(JsonNode engine, Map<String, String> props) {
        if (engine == null) return;

        setIfPresent(props, "floe.engine-type", engine.get("type"));

        // Livy configuration
        JsonNode livy = engine.get("livy");
        if (livy != null) {
            setIfPresent(props, "floe.livy.url", livy.get("url"));
            setIfPresent(props, "floe.livy.driver-memory", livy.get("driverMemory"));
            setIfPresent(props, "floe.livy.executor-memory", livy.get("executorMemory"));

            JsonNode job = livy.get("job");
            if (job != null) {
                setIfPresent(props, "floe.livy.job.jar", job.get("jar"));
                setIfPresent(props, "floe.livy.job.main-class", job.get("mainClass"));
            }
        }

        // Trino configuration
        JsonNode trino = engine.get("trino");
        if (trino != null) {
            setIfPresent(props, "floe.trino.jdbc-url", trino.get("jdbcUrl"));
            setIfPresent(props, "floe.trino.user", trino.get("user"));
            setIfPresent(props, "floe.trino.password", trino.get("password"));
            setIfPresent(props, "floe.trino.catalog", trino.get("catalog"));
            setIfPresent(props, "floe.trino.schema", trino.get("schema"));
            setIfPresent(
                    props, "floe.trino.query-timeout-seconds", trino.get("queryTimeoutSeconds"));
        }
    }

    // Store Configuration Mapping

    private static void mapStoreConfig(JsonNode store, Map<String, String> props) {
        if (store == null) return;

        setIfPresent(props, "floe.store.type", store.get("type"));

        JsonNode postgres = store.get("postgres");
        if (postgres != null) {
            // Map to both floe.store.postgres and quarkus.datasource for compatibility
            String jdbcUrl = getTextOrNull(postgres.get("jdbcUrl"));
            String username = getTextOrNull(postgres.get("username"));
            String password = getTextOrNull(postgres.get("password"));

            if (jdbcUrl != null) {
                props.put("floe.store.postgres.jdbc-url", jdbcUrl);
                props.put("quarkus.datasource.jdbc.url", jdbcUrl);
            }
            if (username != null) {
                props.put("floe.store.postgres.username", username);
                props.put("quarkus.datasource.username", username);
            }
            if (password != null) {
                props.put("floe.store.postgres.password", password);
                props.put("quarkus.datasource.password", password);
            }

            // Connection pool settings
            if (postgres.has("minPoolSize")) {
                props.put("quarkus.datasource.jdbc.min-size", postgres.get("minPoolSize").asText());
            }
            if (postgres.has("maxPoolSize")) {
                props.put("quarkus.datasource.jdbc.max-size", postgres.get("maxPoolSize").asText());
            }
        }
    }

    // Scheduler Configuration Mapping

    private static void mapSchedulerConfig(JsonNode scheduler, Map<String, String> props) {
        if (scheduler == null) return;

        if (scheduler.has("enabled")) {
            props.put("floe.scheduler.enabled", scheduler.get("enabled").asText());
        }
        if (scheduler.has("distributedLock")) {
            props.put(
                    "floe.scheduler.distributed-lock-enabled",
                    scheduler.get("distributedLock").asText());
        }
    }

    // Auth Configuration Mapping

    private static void mapAuthConfig(JsonNode auth, Map<String, String> props) {
        if (auth == null) return;

        if (auth.has("enabled")) {
            props.put("floe.auth.enabled", auth.get("enabled").asText());
        }
        setIfPresent(props, "floe.auth.header-name", auth.get("headerName"));
        setIfPresent(props, "floe.auth.bootstrap-key", auth.get("bootstrapKey"));
        setIfPresent(props, "floe.auth.bootstrap-key-name", auth.get("bootstrapKeyName"));

        // Role mapping
        JsonNode roleMapping = auth.get("roleMapping");
        if (roleMapping != null && roleMapping.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = roleMapping.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                props.put("floe.auth.role-mapping." + entry.getKey(), entry.getValue().asText());
            }
        }
    }

    // OIDC Configuration Mapping

    private static void mapOidcConfig(JsonNode oidc, Map<String, String> props) {
        if (oidc == null) return;

        if (oidc.has("enabled")) {
            props.put("quarkus.oidc.tenant-enabled", oidc.get("enabled").asText());
        }
        setIfPresent(props, "quarkus.oidc.auth-server-url", oidc.get("authServerUrl"));
        setIfPresent(props, "quarkus.oidc.client-id", oidc.get("clientId"));
        setIfPresent(props, "quarkus.oidc.credentials.secret", oidc.get("clientSecret"));
        setIfPresent(props, "quarkus.oidc.token.issuer", oidc.get("issuer"));
        setIfPresent(props, "quarkus.oidc.token.audience", oidc.get("audience"));
        setIfPresent(props, "quarkus.oidc.roles.role-claim-path", oidc.get("roleClaimPath"));
    }

    // Security Configuration Mapping

    private static void mapSecurityConfig(JsonNode security, Map<String, String> props) {
        if (security == null) return;

        // Audit
        JsonNode audit = security.get("audit");
        if (audit != null) {
            if (audit.has("enabled")) {
                props.put("floe.security.audit.enabled", audit.get("enabled").asText());
            }
            if (audit.has("databaseEnabled")) {
                props.put(
                        "floe.security.audit.database-enabled",
                        audit.get("databaseEnabled").asText());
            }
            if (audit.has("retentionDays")) {
                props.put(
                        "floe.security.audit.retention-days", audit.get("retentionDays").asText());
            }
            setIfPresent(props, "floe.security.audit.log-path", audit.get("logPath"));

            // Archival
            JsonNode archival = audit.get("archival");
            if (archival != null) {
                if (archival.has("enabled")) {
                    props.put(
                            "floe.security.audit.archival-enabled",
                            archival.get("enabled").asText());
                }
                if (archival.has("thresholdDays")) {
                    props.put(
                            "floe.security.audit.archival-threshold-days",
                            archival.get("thresholdDays").asText());
                }
                setIfPresent(props, "floe.security.audit.archival-bucket", archival.get("bucket"));
            }
        }
    }

    // Events Configuration Mapping

    private static void mapEventsConfig(JsonNode events, Map<String, String> props) {
        if (events == null) return;

        if (events.has("enabled")) {
            props.put("floe.events.enabled", events.get("enabled").asText());
        }
        setIfPresent(props, "floe.events.type", events.get("type"));
    }

    // Server Configuration Mapping

    private static void mapServerConfig(JsonNode server, Map<String, String> props) {
        if (server == null) return;

        if (server.has("port")) {
            props.put("quarkus.http.port", server.get("port").asText());
        }
    }

    // Logging Configuration Mapping

    private static void mapLoggingConfig(JsonNode logging, Map<String, String> props) {
        if (logging == null) return;

        setIfPresent(props, "quarkus.log.level", logging.get("level"));

        JsonNode categories = logging.get("categories");
        if (categories != null && categories.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = categories.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                props.put(
                        "quarkus.log.category.\"" + entry.getKey() + "\".level",
                        entry.getValue().asText());
            }
        }
    }

    // Helper Methods

    private static void setIfPresent(Map<String, String> props, String key, JsonNode value) {
        String text = getTextOrNull(value);
        if (text != null) {
            props.put(key, text);
        }
    }

    private static String getTextOrNull(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        String text = node.asText();
        return text.isBlank() ? null : text;
    }
}
