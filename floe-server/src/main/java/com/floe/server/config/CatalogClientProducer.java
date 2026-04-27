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

import com.floe.core.catalog.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.*;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** CDI producer for CatalogClient. Supports REST, Hive Metastore, and custom REST catalogs. */
@ApplicationScoped
public class CatalogClientProducer {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogClientProducer.class);

    @Inject FloeConfig config;

    @Produces
    @Singleton
    public CatalogClient produceCatalogClient() {
        FloeConfig.Catalog catalogConfig = config.catalog();
        String catalogType = catalogConfig.type().toUpperCase(Locale.ROOT);
        String catalogName = catalogConfig.name();
        String warehouse = catalogConfig.warehouse();

        LOG.info("Creating {} catalog client: name={}", catalogType, catalogName);

        try {
            return switch (catalogType) {
                case "REST" -> createRestCatalogClient(catalogConfig);
                case "HIVE" -> createHiveCatalogClient(catalogConfig);
                case "POLARIS" -> createPolarisCatalogClient(catalogConfig);
                case "NESSIE" -> createNessieCatalogClient(catalogConfig);
                case "LAKEKEEPER" -> createLakekeeperCatalogClient(catalogConfig);
                case "GRAVITINO" -> createGravitinoCatalogClient(catalogConfig);
                case "DATAHUB" -> createDataHubCatalogClient(catalogConfig);
                default -> {
                    LOG.error(
                            "Unsupported catalog type: {}. Supported types: REST, HIVE, POLARIS, NESSIE, LAKEKEEPER, GRAVITINO, DATAHUB",
                            catalogType);
                    yield new StubCatalogClient(catalogName);
                }
            };
        } catch (Exception e) {
            LOG.warn(
                    "Catalog unavailable ({}), using stub client: {}", catalogType, e.getMessage());
            return new StubCatalogClient(catalogName);
        }
    }

    private CatalogClient createRestCatalogClient(FloeConfig.Catalog catalogConfig) {
        String catalogName = catalogConfig.name();
        String uri = catalogConfig.uri();
        String warehouse = catalogConfig.warehouse();

        Map<String, String> additionalProps = new HashMap<>();

        // Add S3 configuration if endpoint is specified
        FloeConfig.Catalog.S3 s3Config = catalogConfig.s3();
        s3Config.endpoint()
                .filter(ep -> !ep.isBlank())
                .ifPresent(
                        endpoint -> {
                            additionalProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                            additionalProps.put("s3.endpoint", endpoint);
                            additionalProps.put("s3.access-key-id", s3Config.accessKeyId());
                            additionalProps.put("s3.secret-access-key", s3Config.secretAccessKey());
                            additionalProps.put("s3.region", s3Config.region());
                            additionalProps.put("s3.path-style-access", "true");
                        });

        LOG.info("Creating REST catalog client: uri={}, warehouse={}", uri, warehouse);

        return IcebergRestCatalogClient.builder()
                .catalogName(catalogName)
                .uri(uri)
                .warehouse(warehouse)
                .additionalProperties(additionalProps)
                .build();
    }

    private CatalogClient createHiveCatalogClient(FloeConfig.Catalog catalogConfig) {
        String catalogName = catalogConfig.name();
        String warehouse = catalogConfig.warehouse();
        FloeConfig.Catalog.Hive hiveConfig = catalogConfig.hive();

        String metastoreUri =
                hiveConfig
                        .uri()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "floe.catalog.hive.uri is required when catalog type is HIVE"));

        Map<String, String> additionalProps = new HashMap<>();

        // Add S3 configuration if endpoint is specified
        FloeConfig.Catalog.S3 s3Config = catalogConfig.s3();
        s3Config.endpoint()
                .filter(ep -> !ep.isBlank())
                .ifPresent(
                        endpoint -> {
                            additionalProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                            additionalProps.put("s3.endpoint", endpoint);
                            additionalProps.put("s3.access-key-id", s3Config.accessKeyId());
                            additionalProps.put("s3.secret-access-key", s3Config.secretAccessKey());
                            additionalProps.put("s3.region", s3Config.region());
                            additionalProps.put("s3.path-style-access", "true");

                            // Hadoop S3A configuration
                            additionalProps.put(
                                    "hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                            additionalProps.put("hadoop.fs.s3a.endpoint", endpoint);
                            additionalProps.put("hadoop.fs.s3a.access.key", s3Config.accessKeyId());
                            additionalProps.put(
                                    "hadoop.fs.s3a.secret.key", s3Config.secretAccessKey());
                            additionalProps.put("hadoop.fs.s3a.path.style.access", "true");
                        });

        LOG.info(
                "Creating Hive Metastore catalog client: uri={}, warehouse={}",
                metastoreUri,
                warehouse);

        return new HiveMetastoreCatalogClient(
                catalogName, metastoreUri, warehouse, additionalProps);
    }

    private CatalogClient createPolarisCatalogClient(FloeConfig.Catalog catalogConfig) {
        String catalogName = catalogConfig.name();
        String warehouse = catalogConfig.warehouse();
        FloeConfig.Catalog.Polaris polarisConfig = catalogConfig.polaris();

        String polarisUri =
                polarisConfig
                        .uri()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "floe.catalog.polaris.uri is required when catalog type is POLARIS"));

        String clientId =
                polarisConfig
                        .clientId()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "floe.catalog.polaris.client-id is required when catalog type is POLARIS"));

        String clientSecret =
                polarisConfig
                        .clientSecret()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "floe.catalog.polaris.client-secret is required when catalog type is POLARIS"));

        String principalRole = polarisConfig.principalRole().orElse("");

        Map<String, String> additionalProps = new HashMap<>();

        // Add S3 configuration if endpoint is specified
        FloeConfig.Catalog.S3 s3Config = catalogConfig.s3();
        s3Config.endpoint()
                .filter(ep -> !ep.isBlank())
                .ifPresent(
                        endpoint -> {
                            additionalProps.put("s3.endpoint", endpoint);
                            additionalProps.put("s3.access-key-id", s3Config.accessKeyId());
                            additionalProps.put("s3.secret-access-key", s3Config.secretAccessKey());
                            additionalProps.put("s3.region", s3Config.region());
                            additionalProps.put("s3.path-style-access", "true");
                        });

        LOG.info(
                "Creating Polaris catalog client: uri={}, warehouse={}, principalRole={}",
                polarisUri,
                warehouse,
                principalRole.isEmpty() ? "<none>" : principalRole);

        return PolarisCatalogClient.builder()
                .catalogName(catalogName)
                .polarisUri(polarisUri)
                .warehouse(warehouse)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .principalRole(principalRole)
                .additionalProperties(additionalProps)
                .build();
    }

    private CatalogClient createNessieCatalogClient(FloeConfig.Catalog catalogConfig) {
        String catalogName = catalogConfig.name();
        String warehouse = catalogConfig.warehouse();
        FloeConfig.Catalog.Nessie nessieConfig = catalogConfig.nessie();

        String nessieUri =
                nessieConfig
                        .uri()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "floe.catalog.nessie.uri is required when catalog type is NESSIE"));

        String ref = nessieConfig.ref();
        String token = nessieConfig.token().orElse(null);

        Map<String, String> additionalProps = new HashMap<>();

        // Add S3 configuration if endpoint is specified
        FloeConfig.Catalog.S3 s3Config = catalogConfig.s3();
        s3Config.endpoint()
                .filter(ep -> !ep.isBlank())
                .ifPresent(
                        endpoint -> {
                            // io-impl is required for NessieCatalog to read from S3
                            additionalProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                            additionalProps.put("s3.endpoint", endpoint);
                            additionalProps.put("s3.access-key-id", s3Config.accessKeyId());
                            additionalProps.put("s3.secret-access-key", s3Config.secretAccessKey());
                            additionalProps.put("s3.region", s3Config.region());
                            additionalProps.put("s3.path-style-access", "true");
                        });

        LOG.info(
                "Creating Nessie catalog client: uri={}, ref={}, warehouse={}",
                nessieUri,
                ref,
                warehouse);

        return NessieCatalogClient.builder()
                .catalogName(catalogName)
                .nessieUri(nessieUri)
                .warehouse(warehouse)
                .ref(ref)
                .bearerToken(token)
                .additionalProperties(additionalProps)
                .build();
    }

    private CatalogClient createLakekeeperCatalogClient(FloeConfig.Catalog catalogConfig) {
        String catalogName = catalogConfig.name();
        String warehouse = catalogConfig.warehouse();
        FloeConfig.Catalog.Lakekeeper lakekeeperConfig = catalogConfig.lakekeeper();

        String uri =
                lakekeeperConfig
                        .uri()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "floe.catalog.lakekeeper.uri is required when catalog type is LAKEKEEPER"));

        Map<String, String> additionalProps = new HashMap<>();

        // Add S3 configuration if endpoint is specified (for non-vended credentials scenarios)
        FloeConfig.Catalog.S3 s3Config = catalogConfig.s3();
        s3Config.endpoint()
                .filter(ep -> !ep.isBlank())
                .ifPresent(
                        endpoint -> {
                            additionalProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                            additionalProps.put("s3.endpoint", endpoint);
                            additionalProps.put("s3.access-key-id", s3Config.accessKeyId());
                            additionalProps.put("s3.secret-access-key", s3Config.secretAccessKey());
                            additionalProps.put("s3.region", s3Config.region());
                            additionalProps.put("s3.path-style-access", "true");
                        });

        LOG.info("Creating Lakekeeper catalog client: uri={}, warehouse={}", uri, warehouse);

        // Build the REST catalog client with OAuth2 if configured
        var builder =
                IcebergRestCatalogClient.builder()
                        .catalogName(catalogName)
                        .uri(uri)
                        .warehouse(warehouse)
                        .additionalProperties(additionalProps);

        // Configure OAuth2 if credentials are provided
        lakekeeperConfig
                .credential()
                .filter(c -> !c.isBlank() && c.contains(":"))
                .ifPresent(
                        credential -> {
                            String[] parts = credential.split(":", 2);
                            String clientId = parts[0];
                            String clientSecret = parts[1];
                            String tokenEndpoint =
                                    lakekeeperConfig
                                            .oauth2ServerUri()
                                            .orElse(uri + "/v1/oauth/tokens");
                            builder.oauth2(clientId, clientSecret, tokenEndpoint);
                            lakekeeperConfig.scope().ifPresent(builder::scope);
                        });

        return builder.build();
    }

    private CatalogClient createGravitinoCatalogClient(FloeConfig.Catalog catalogConfig) {
        String catalogName = catalogConfig.name();
        String warehouse = catalogConfig.warehouse();
        FloeConfig.Catalog.Gravitino gravitinoConfig = catalogConfig.gravitino();

        String uri =
                gravitinoConfig
                        .uri()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "floe.catalog.gravitino.uri is required when catalog type is GRAVITINO"));

        Map<String, String> additionalProps = new HashMap<>();

        // Add S3 configuration if endpoint is specified
        FloeConfig.Catalog.S3 s3Config = catalogConfig.s3();
        s3Config.endpoint()
                .filter(ep -> !ep.isBlank())
                .ifPresent(
                        endpoint -> {
                            additionalProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                            additionalProps.put("s3.endpoint", endpoint);
                            additionalProps.put("s3.access-key-id", s3Config.accessKeyId());
                            additionalProps.put("s3.secret-access-key", s3Config.secretAccessKey());
                            additionalProps.put("s3.region", s3Config.region());
                            additionalProps.put("s3.path-style-access", "true");
                        });

        LOG.info("Creating Gravitino catalog client: uri={}, warehouse={}", uri, warehouse);

        // Build the REST catalog client with OAuth2 if configured
        var builder =
                IcebergRestCatalogClient.builder()
                        .catalogName(catalogName)
                        .uri(uri)
                        .warehouse(warehouse)
                        .additionalProperties(additionalProps);

        // Configure OAuth2 if credentials are provided
        gravitinoConfig
                .credential()
                .filter(c -> !c.isBlank() && c.contains(":"))
                .ifPresent(
                        credential -> {
                            String[] parts = credential.split(":", 2);
                            String clientId = parts[0];
                            String clientSecret = parts[1];
                            String tokenEndpoint =
                                    gravitinoConfig
                                            .oauth2ServerUri()
                                            .orElse(uri + "/v1/oauth/tokens");
                            builder.oauth2(clientId, clientSecret, tokenEndpoint);
                        });

        return builder.build();
    }

    private CatalogClient createDataHubCatalogClient(FloeConfig.Catalog catalogConfig) {
        String catalogName = catalogConfig.name();
        String warehouse = catalogConfig.warehouse();
        FloeConfig.Catalog.DataHub datahubConfig = catalogConfig.datahub();

        String uri =
                datahubConfig
                        .uri()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "floe.catalog.datahub.uri is required when catalog type is DATAHUB"));

        String token =
                datahubConfig
                        .token()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "floe.catalog.datahub.token is required when catalog type is DATAHUB"));

        Map<String, String> additionalProps = new HashMap<>();

        // Add S3 configuration if endpoint is specified
        FloeConfig.Catalog.S3 s3Config = catalogConfig.s3();
        s3Config.endpoint()
                .filter(ep -> !ep.isBlank())
                .ifPresent(
                        endpoint -> {
                            additionalProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                            additionalProps.put("s3.endpoint", endpoint);
                            additionalProps.put("s3.access-key-id", s3Config.accessKeyId());
                            additionalProps.put("s3.secret-access-key", s3Config.secretAccessKey());
                            additionalProps.put("s3.region", s3Config.region());
                            additionalProps.put("s3.path-style-access", "true");
                        });

        LOG.info("Creating DataHub catalog client: uri={}, warehouse={}", uri, warehouse);

        return DataHubCatalogClient.builder()
                .catalogName(catalogName)
                .datahubUri(uri)
                .warehouse(warehouse)
                .token(token)
                .additionalProperties(additionalProps)
                .build();
    }

    /** Stub catalog client when real catalog is unavailable or misconfigured. */
    private static class StubCatalogClient implements CatalogClient {

        private final String name;

        StubCatalogClient(String name) {
            this.name = name;
            LOG.warn(
                    "Using stub catalog client for '{}' - catalog operations will return empty results",
                    name);
        }

        @Override
        public String getCatalogName() {
            return name;
        }

        @Override
        public boolean isHealthy() {
            return false;
        }

        @Override
        public List<String> listNamespaces() {
            return List.of();
        }

        @Override
        public List<TableIdentifier> listTables(String namespace) {
            return List.of();
        }

        @Override
        public List<TableIdentifier> listAllTables() {
            return List.of();
        }

        @Override
        public Optional<Table> loadTable(TableIdentifier identifier) {
            return Optional.empty();
        }

        @Override
        public Optional<TableMetadata> getTableMetadata(TableIdentifier identifier) {
            return Optional.empty();
        }

        @Override
        public void close() {}
    }
}
