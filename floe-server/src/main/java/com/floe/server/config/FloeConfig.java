package com.floe.server.config;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Map;
import java.util.Optional;

/**
 * Type-safe configuration for Floe.
 *
 * <p>Maps to properties prefixed with "floe." in application.properties.
 */
@ConfigMapping(prefix = "floe")
@StaticInitSafe
public interface FloeConfig {
    // Engine Configuration

    @WithDefault("SPARK")
    String engineType();

    Livy livy();

    Trino trino();

    Catalog catalog();

    Store store();

    Auth auth();

    Security security();

    Optional<Events> events();

    // Nested Interfaces

    interface Livy {
        @WithDefault("http://localhost:8998")
        String url();

        Job job();

        @WithDefault("2g")
        String driverMemory();

        @WithDefault("2g")
        String executorMemory();

        interface Job {
            @WithDefault("local:///opt/floe/floe-maintenance-job.jar")
            String jar();

            @WithDefault("com.floe.spark.job.MaintenanceJob")
            String mainClass();
        }
    }

    interface Trino {
        @WithDefault("jdbc:trino://localhost:8080")
        String jdbcUrl();

        @WithDefault("floe")
        String user();

        Optional<String> password();

        @WithDefault("iceberg")
        String catalog();

        Optional<String> schema();

        @WithDefault("3600")
        int queryTimeoutSeconds();
    }

    interface Catalog {
        /** Catalog type: REST, HIVE, POLARIS, NESSIE */
        @WithDefault("REST")
        String type();

        @WithDefault("demo")
        String name();

        /** REST catalog URI (used when type=REST) */
        @WithDefault("http://localhost:8181")
        String uri();

        @WithDefault("s3://warehouse/")
        String warehouse();

        S3 s3();

        /** Hive Metastore configuration (used when type=HIVE) */
        Hive hive();

        /** Apache Polaris configuration (used when type=POLARIS) */
        Polaris polaris();

        /** Project Nessie configuration (used when type=NESSIE) */
        Nessie nessie();

        interface S3 {
            Optional<String> endpoint();

            @WithDefault("")
            String accessKeyId();

            @WithDefault("")
            String secretAccessKey();

            @WithDefault("us-east-1")
            String region();
        }

        interface Hive {
            /** Hive Metastore URI (e.g., thrift://localhost:9083) */
            Optional<String> uri();

            /** Path to hive-site.xml (optional, for additional Hive config) */
            Optional<String> configPath();
        }

        /**
         * Apache Polaris configuration. Polaris uses OAuth2 client credentials for authentication.
         */
        interface Polaris {
            /** Polaris server URI (e.g., https://polaris.example.com) */
            Optional<String> uri();

            /** OAuth2 client ID */
            Optional<String> clientId();

            /** OAuth2 client secret */
            Optional<String> clientSecret();

            /** Polaris principal role for scoped access */
            Optional<String> principalRole();
        }

        /** Project Nessie configuration. Nessie uses Bearer token (OIDC/JWT) for authentication. */
        interface Nessie {
            /** Nessie server URI (e.g., https://nessie.example.com/api/v1) */
            Optional<String> uri();

            /** Git reference (branch or tag), defaults to "main" */
            @WithDefault("main")
            String ref();

            /** Bearer token for authentication (optional, can use Floe's OIDC token) */
            Optional<String> token();
        }
    }

    interface Store {
        @WithDefault("MEMORY")
        String type();

        Postgres postgres();

        interface Postgres {
            Optional<String> jdbcUrl();

            Optional<String> username();

            Optional<String> password();
        }
    }

    interface Auth {
        /** Enable or disable authentication. Enabled by default for production. */
        @WithDefault("true")
        boolean enabled();

        /** Bootstrap admin API key for initial setup. Only used if no keys exist. */
        Optional<String> bootstrapKey();

        /** Name for the bootstrap key. */
        @WithDefault("Bootstrap Admin")
        String bootstrapKeyName();

        /** Header name for API key authentication. */
        @WithDefault("X-API-Key")
        String headerName();

        /**
         * Custom role mapping from external identity provider roles to Floe roles. Format:
         * floe.auth.role-mapping.<external-role>=<ADMIN|OPERATOR|VIEWER>
         */
        @WithDefault("{}")
        Map<String, String> roleMapping();
    }

    interface Security {
        Audit audit();

        interface Audit {
            /** Enable audit logging for security events. */
            @WithDefault("true")
            boolean enabled();

            /** Log file path for audit logs (optional, uses standard logging if not set). */
            Optional<String> logPath();

            /** Enable database storage for audit logs (for compliance and queryability). */
            @WithDefault("true")
            boolean databaseEnabled();

            /**
             * Audit log retention period in days. Logs older than this will be archived/deleted.
             * Default: 2555 days (7 years, GDPR/SOC 2 requirement)
             */
            @WithDefault("2555")
            int retentionDays();

            /**
             * Enable automatic archival of old audit logs to object storage (S3, etc). When
             * enabled, logs older than archivalThresholdDays are moved to cheaper storage.
             */
            @WithDefault("false")
            boolean archivalEnabled();

            /**
             * Threshold in days after which audit logs are archived to object storage. Default: 90
             * days (keep 3 months in database for fast queries)
             */
            @WithDefault("90")
            int archivalThresholdDays();

            /**
             * S3 bucket for archived audit logs. Required if archivalEnabled=true. Format:
             * s3://bucket-name/path/
             */
            Optional<String> archivalBucket();
        }
    }

    /**
     * Event emission configuration. Events are emitted for maintenance operations and policy
     * changes.
     */
    interface Events {
        /** Enable/disable event emission. Default: true (events are logged) */
        @WithDefault("true")
        boolean enabled();

        /** Event emitter type. Default: logging (events logged via SLF4J) */
        @WithDefault("logging")
        String type();
    }
}
