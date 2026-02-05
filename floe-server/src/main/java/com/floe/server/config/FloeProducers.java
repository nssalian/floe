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

import com.floe.core.auth.*;
import com.floe.core.catalog.CatalogConfigStore;
import com.floe.core.catalog.InMemoryCatalogConfigStore;
import com.floe.core.catalog.PostgresCatalogConfigStore;
import com.floe.core.engine.ExecutionEngine;
import com.floe.core.event.EventEmitter;
import com.floe.core.event.LoggingEventEmitter;
import com.floe.core.health.InMemoryTableHealthStore;
import com.floe.core.health.PostgresTableHealthStore;
import com.floe.core.health.TableHealthStore;
import com.floe.core.metrics.OperationMetricsEmitter;
import com.floe.core.operation.InMemoryOperationStore;
import com.floe.core.operation.OperationStore;
import com.floe.core.operation.PostgresOperationStore;
import com.floe.core.orchestrator.MaintenanceOrchestrator;
import com.floe.core.policy.InMemoryPolicyStore;
import com.floe.core.policy.PolicyMatcher;
import com.floe.core.policy.PolicyStore;
import com.floe.core.policy.PostgresPolicyStore;
import com.floe.core.scheduler.*;
import com.floe.engine.spark.SparkEngineConfig;
import com.floe.engine.spark.SparkExecutionEngine;
import com.floe.engine.trino.TrinoEngineConfig;
import com.floe.engine.trino.TrinoExecutionEngine;
import com.floe.server.metrics.FloeMetrics;
import com.floe.server.scheduler.SchedulerConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** CDI Producer for core Floe services. */
@ApplicationScoped
public class FloeProducers {

    private static final Logger LOG = LoggerFactory.getLogger(FloeProducers.class);

    @Inject FloeConfigProvider configProvider;

    @Inject HealthConfig healthConfig;

    @Inject SchedulerConfig schedulerConfig;

    private FloeConfig config() {
        return configProvider.get();
    }

    // Quarkus-managed DataSource (Agroal)
    @Inject DataSource dataSource;

    // Store Beans

    @Produces
    @Singleton
    public PolicyStore policyStore() {
        String storeType = config().store().type();
        LOG.info("Initializing policy store: {}", storeType);

        return switch (storeType.toUpperCase(Locale.ROOT)) {
            case "MEMORY" -> {
                LOG.warn("Using in-memory policy store - data will be lost on restart!");
                yield new InMemoryPolicyStore();
            }
            case "POSTGRES" -> {
                var store = new PostgresPolicyStore(dataSource);
                store.initializeSchema();
                yield store;
            }
            default ->
                    throw new IllegalArgumentException(
                            "Unsupported store type: "
                                    + storeType
                                    + ". Supported: MEMORY, POSTGRES");
        };
    }

    @Produces
    @Singleton
    public OperationStore operationStore() {
        String storeType = config().store().type();
        LOG.info("Initializing operation store: {}", storeType);

        return switch (storeType.toUpperCase(Locale.ROOT)) {
            case "MEMORY" -> {
                LOG.warn("Using in-memory operation store - data will be lost on restart!");
                yield new InMemoryOperationStore();
            }
            case "POSTGRES" -> {
                var store = new PostgresOperationStore(dataSource);
                store.initializeSchema();
                yield store;
            }
            default ->
                    throw new IllegalArgumentException(
                            "Unsupported store type: "
                                    + storeType
                                    + ". Supported: MEMORY, POSTGRES");
        };
    }

    @Produces
    @Singleton
    public ScheduleExecutionStore scheduleExecutionStore() {
        String storeType = config().store().type();
        LOG.info("Initializing schedule execution store: {}", storeType);

        return switch (storeType.toUpperCase(Locale.ROOT)) {
            case "MEMORY" -> {
                LOG.warn(
                        "Using in-memory schedule execution store - data will be lost on restart!");
                yield new InMemoryScheduleExecutionStore();
            }
            case "POSTGRES" -> {
                var store = new PostgresScheduleExecutionStore(dataSource);
                store.initializeSchema();
                yield store;
            }
            default ->
                    throw new IllegalArgumentException(
                            "Unsupported store type: "
                                    + storeType
                                    + ". Supported: MEMORY, POSTGRES");
        };
    }

    @Produces
    @Singleton
    public CatalogConfigStore catalogConfigStore() {
        String storeType = config().store().type();
        LOG.info("Initializing catalog config store: {}", storeType);

        return switch (storeType.toUpperCase(Locale.ROOT)) {
            case "MEMORY" -> {
                LOG.warn("Using in-memory catalog config store - data will be lost on restart!");
                yield new InMemoryCatalogConfigStore();
            }
            case "POSTGRES" -> {
                var store = new PostgresCatalogConfigStore(dataSource);
                store.initializeSchema();
                yield store;
            }
            default ->
                    throw new IllegalArgumentException(
                            "Unsupported store type: "
                                    + storeType
                                    + ". Supported: MEMORY, POSTGRES");
        };
    }

    @Produces
    @Singleton
    public TableHealthStore tableHealthStore() {
        String storeType = config().store().type();
        boolean enabled = healthConfig.persistenceEnabled();

        if (!enabled) {
            LOG.info("Health report persistence disabled");
            return new InMemoryTableHealthStore(); // Non-persisting fallback
        }

        LOG.info("Initializing table health store: {}", storeType);

        return switch (storeType.toUpperCase(Locale.ROOT)) {
            case "MEMORY" -> {
                LOG.warn("Using in-memory health store - data will be lost on restart!");
                yield new InMemoryTableHealthStore();
            }
            case "POSTGRES" -> {
                var store = new PostgresTableHealthStore(dataSource);
                store.initializeSchema();
                yield store;
            }
            default ->
                    throw new IllegalArgumentException(
                            "Unsupported store type: "
                                    + storeType
                                    + ". Supported: MEMORY, POSTGRES");
        };
    }

    @Produces
    @Singleton
    public DistributedLock distributedLock() {
        String storeType = config().store().type();
        boolean enabled = schedulerConfig.distributedLockEnabled();

        if (!enabled) {
            LOG.info("Distributed locking disabled, using no-op lock");
            return new DistributedLock.NoOpDistributedLock();
        }

        LOG.info("Initializing distributed lock: {}", storeType);

        return switch (storeType.toUpperCase(Locale.ROOT)) {
            case "MEMORY" -> {
                LOG.warn("Distributed locking enabled but using MEMORY store - using no-op lock");
                yield new DistributedLock.NoOpDistributedLock();
            }
            case "POSTGRES" -> {
                LOG.info("Using Postgres advisory locks for distributed locking");
                yield new PostgresDistributedLock(dataSource);
            }
            default -> {
                LOG.warn(
                        "Distributed locking enabled but store type {} not supported - using no-op lock",
                        storeType);
                yield new DistributedLock.NoOpDistributedLock();
            }
        };
    }

    @Produces
    @Singleton
    public ApiKeyStore apiKeyStore() {
        String storeType = config().store().type();
        LOG.info("Initializing API key store: {}", storeType);

        ApiKeyStore store =
                switch (storeType.toUpperCase(Locale.ROOT)) {
                    case "MEMORY" -> {
                        LOG.warn("Using in-memory API key store - data will be lost on restart!");
                        yield new InMemoryApiKeyStore();
                    }
                    case "POSTGRES" -> {
                        var pgStore = new PostgresApiKeyStore(dataSource);
                        pgStore.initializeSchema();
                        yield pgStore;
                    }
                    default ->
                            throw new IllegalArgumentException(
                                    "Unsupported store type: "
                                            + storeType
                                            + ". Supported: MEMORY, POSTGRES");
                };

        initializeBootstrapKey(store);
        return store;
    }

    private void initializeBootstrapKey(ApiKeyStore store) {
        if (store.count() > 0) {
            LOG.debug("API keys already exist, skipping bootstrap key initialization");
            return;
        }

        config().auth()
                .bootstrapKey()
                .filter(key -> !key.isBlank())
                .ifPresent(
                        bootstrapKey -> {
                            if (!ApiKeyGenerator.isValidKeyFormat(bootstrapKey)) {
                                LOG.warn("Bootstrap key has non-standard format, using as-is");
                            }

                            String keyHash = ApiKeyGenerator.hashKey(bootstrapKey);
                            ApiKey apiKey =
                                    ApiKey.builder()
                                            .keyHash(keyHash)
                                            .name(config().auth().bootstrapKeyName())
                                            .role(Role.ADMIN)
                                            .build();

                            store.save(apiKey);
                            LOG.info(
                                    "Initialized bootstrap admin API key: {}",
                                    config().auth().bootstrapKeyName());
                        });
    }

    // Policy Beans

    @Produces
    @Singleton
    public PolicyMatcher policyMatcher(PolicyStore policyStore) {
        return new PolicyMatcher(policyStore);
    }

    // Orchestrator Bean

    @Produces
    @Singleton
    public MaintenanceOrchestrator orchestrator(
            PolicyStore policyStore,
            PolicyMatcher policyMatcher,
            ExecutionEngine executionEngine,
            OperationStore operationStore,
            FloeMetrics metrics) {
        OperationMetricsEmitter emitter =
                (operationType, status, duration) ->
                        metrics.recordOperationExecution(operationType, status, duration);
        return new MaintenanceOrchestrator(
                policyStore,
                policyMatcher,
                executionEngine,
                operationStore,
                java.util.concurrent.Executors.newFixedThreadPool(4),
                new com.floe.core.orchestrator.MaintenancePlanner(),
                emitter);
    }

    // Execution Engine Bean

    @Produces
    @Singleton
    public ExecutionEngine executionEngine() {
        String engineType = config().engineType();
        LOG.info("Initializing execution engine: {}", engineType);

        return switch (engineType.toUpperCase(Locale.ROOT)) {
            case "SPARK" -> createSparkEngine();
            case "TRINO" -> createTrinoEngine();
            default ->
                    throw new IllegalArgumentException(
                            "Unsupported engine type: " + engineType + ". Supported: SPARK, TRINO");
        };
    }

    private SparkExecutionEngine createSparkEngine() {
        // Execution via Livy
        var livy = config().livy();
        LOG.info("Creating Spark engine via Livy: url={}, jar={}", livy.url(), livy.job().jar());

        return new SparkExecutionEngine(
                SparkEngineConfig.builder()
                        .livyUrl(livy.url())
                        .maintenanceJobJar(livy.job().jar())
                        .maintenanceJobClass(livy.job().mainClass())
                        .catalogProperties(buildCatalogProperties())
                        .sparkConf(buildSparkConf())
                        .driverMemory(livy.driverMemory())
                        .executorMemory(livy.executorMemory())
                        .pollIntervalMs(2000)
                        .defaultTimeoutSeconds(3600)
                        .build());
    }

    private TrinoExecutionEngine createTrinoEngine() {
        var trino = config().trino();
        var catalogType = config().catalog().type();
        LOG.info(
                "Creating Trino execution engine: {}, catalogType={}",
                trino.jdbcUrl(),
                catalogType);

        return new TrinoExecutionEngine(
                TrinoEngineConfig.builder()
                        .jdbcUrl(trino.jdbcUrl())
                        .username(trino.user())
                        .password(trino.password().orElse(null))
                        .catalog(trino.catalog())
                        .schema(trino.schema().orElse(null))
                        .catalogType(catalogType)
                        .queryTimeoutSeconds(trino.queryTimeoutSeconds())
                        .build());
    }

    // Extension Point Beans

    @Produces
    @Singleton
    public EventEmitter eventEmitter() {
        boolean enabled = config().events().map(e -> e.enabled()).orElse(true);
        LOG.info("Initializing event emitter: enabled={}", enabled);
        return new LoggingEventEmitter(enabled);
    }

    @Produces
    @Singleton
    public AuthorizationProvider authorizationProvider(ApiKeyStore apiKeyStore) {
        LOG.info("Initializing authorization provider: RBAC");
        // Default implementation uses built-in Role/Permission system
        // Future: OpenFgaAuthorizationProvider, SpiceDbAuthorizationProvider,
        // AvpAuthorizationProvider
        return new RbacAuthorizationProvider(
                subject -> {
                    // Resolve subject to role via API key store
                    return apiKeyStore.findByKeyHash(subject).map(ApiKey::role);
                });
    }

    // Configuration Builders

    /**
     * Builds Spark catalog properties based on the configured catalog type.
     *
     * <p>Supports: REST, HIVE/HMS, NESSIE, POLARIS
     */
    private Map<String, String> buildCatalogProperties() {
        var catalog = config().catalog();
        Map<String, String> props = new HashMap<>();
        String catalogType = catalog.type().toUpperCase(Locale.ROOT);

        LOG.info("Building catalog properties for type: {}", catalogType);

        switch (catalogType) {
            case "REST" -> {
                props.put("type", "rest");
                props.put("uri", catalog.uri());
                props.put("warehouse", catalog.warehouse());
            }
            case "HIVE", "HMS" -> {
                props.put("type", "hive");
                catalog.hive()
                        .uri()
                        .ifPresent(
                                uri -> {
                                    props.put("uri", uri);
                                });
                props.put("warehouse", catalog.warehouse());
            }
            case "NESSIE" -> {
                props.put("catalog-impl", "org.apache.iceberg.nessie.NessieCatalog");
                catalog.nessie()
                        .uri()
                        .ifPresent(
                                uri -> {
                                    props.put("uri", uri);
                                });
                props.put("ref", catalog.nessie().ref());
                props.put("warehouse", catalog.warehouse());
                // Nessie auth token if configured
                catalog.nessie()
                        .token()
                        .ifPresent(token -> props.put("authentication.type", "BEARER"));
            }
            case "POLARIS" -> {
                // Polaris is a REST catalog with OAuth2
                props.put("type", "rest");
                catalog.polaris()
                        .uri()
                        .ifPresent(
                                uri -> {
                                    props.put("uri", uri);
                                });
                props.put("warehouse", catalog.warehouse());
                // OAuth2 credentials for Polaris
                var polaris = catalog.polaris();
                if (polaris.clientId().isPresent() && polaris.clientSecret().isPresent()) {
                    props.put(
                            "credential",
                            polaris.clientId().get() + ":" + polaris.clientSecret().get());
                }
                // Set scope - use specified role or default to ALL
                String scope =
                        polaris.principalRole()
                                .filter(role -> !role.isBlank())
                                .map(role -> "PRINCIPAL_ROLE:" + role)
                                .orElse("PRINCIPAL_ROLE:ALL");
                props.put("scope", scope);
            }
            case "LAKEKEEPER" -> {
                // Lakekeeper is an Iceberg REST catalog with OAuth2 and credential vending
                props.put("type", "rest");
                var lakekeeper = catalog.lakekeeper();
                lakekeeper.uri().ifPresent(uri -> props.put("uri", uri));
                props.put("warehouse", catalog.warehouse());
                // OAuth2 credentials if configured
                lakekeeper
                        .credential()
                        .filter(c -> !c.isBlank())
                        .ifPresent(credential -> props.put("credential", credential));
                lakekeeper.oauth2ServerUri().ifPresent(uri -> props.put("oauth2-server-uri", uri));
                lakekeeper.scope().ifPresent(scope -> props.put("scope", scope));
            }
            case "GRAVITINO" -> {
                // Gravitino provides an Iceberg REST catalog interface
                props.put("type", "rest");
                var gravitino = catalog.gravitino();
                gravitino.uri().ifPresent(uri -> props.put("uri", uri));
                props.put("warehouse", catalog.warehouse());
                // OAuth2 credentials if configured
                gravitino
                        .credential()
                        .filter(c -> !c.isBlank())
                        .ifPresent(credential -> props.put("credential", credential));
                gravitino.oauth2ServerUri().ifPresent(uri -> props.put("oauth2-server-uri", uri));
            }
            default ->
                    throw new IllegalArgumentException(
                            "Unsupported catalog type: "
                                    + catalogType
                                    + ". Supported: REST, HIVE, HMS, NESSIE, POLARIS, LAKEKEEPER, GRAVITINO");
        }

        // Add S3 configuration for all catalog types that need it
        addS3Properties(props, catalog);

        LOG.debug("Catalog properties: {} keys configured", props.size());
        return props;
    }

    /** Adds S3/MinIO properties if an endpoint is configured. */
    private void addS3Properties(Map<String, String> props, FloeConfig.Catalog catalog) {
        catalog.s3()
                .endpoint()
                .filter(s -> !s.isBlank())
                .ifPresent(
                        endpoint -> {
                            // Only set io-impl if not already set
                            props.putIfAbsent("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                            props.put("s3.endpoint", endpoint);
                            props.put("s3.access-key-id", catalog.s3().accessKeyId());
                            props.put("s3.secret-access-key", catalog.s3().secretAccessKey());
                            props.put("s3.region", catalog.s3().region());
                            props.put("s3.path-style-access", "true");
                        });
    }

    /** Builds Spark configuration for Hadoop/S3 and catalog-specific settings. */
    private Map<String, String> buildSparkConf() {
        var catalog = config().catalog();
        var s3 = catalog.s3();
        Map<String, String> conf = new HashMap<>();
        String catalogType = catalog.type().toUpperCase(Locale.ROOT);

        // S3/MinIO Hadoop configuration
        s3.endpoint()
                .filter(ep -> !ep.isBlank())
                .ifPresent(
                        endpoint -> {
                            conf.put(
                                    "spark.hadoop.fs.s3.impl",
                                    "org.apache.hadoop.fs.s3a.S3AFileSystem");
                            conf.put(
                                    "spark.hadoop.fs.s3a.impl",
                                    "org.apache.hadoop.fs.s3a.S3AFileSystem");
                            conf.put("spark.hadoop.fs.s3a.endpoint", endpoint);
                            conf.put("spark.hadoop.fs.s3a.access.key", s3.accessKeyId());
                            conf.put("spark.hadoop.fs.s3a.secret.key", s3.secretAccessKey());
                            conf.put("spark.hadoop.fs.s3a.path.style.access", "true");
                            conf.put("spark.hadoop.fs.s3a.connection.ssl.enabled", "false");
                        });

        // Nessie bearer token for authentication
        if ("NESSIE".equals(catalogType)) {
            catalog.nessie()
                    .token()
                    .ifPresent(
                            token -> {
                                conf.put(
                                        "spark.sql.catalog."
                                                + catalog.name()
                                                + ".authentication.type",
                                        "BEARER");
                                conf.put(
                                        "spark.sql.catalog."
                                                + catalog.name()
                                                + ".authentication.token",
                                        token);
                            });
        }

        return conf;
    }
}
