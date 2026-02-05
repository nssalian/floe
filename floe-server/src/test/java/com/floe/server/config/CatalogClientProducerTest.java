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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.floe.core.catalog.CatalogClient;
import java.io.PrintStream;
import java.util.Optional;
import org.junit.jupiter.api.*;

@DisplayName("CatalogClientProducer")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogClientProducerTest {

    // Suppress Iceberg's S3FileIO stderr warnings during tests
    private PrintStream originalStderr;

    @BeforeAll
    void suppressStderr() {
        originalStderr = System.err;
        System.setErr(new PrintStream(java.io.OutputStream.nullOutputStream()));
    }

    @AfterAll
    void restoreStderr() {
        System.setErr(originalStderr);
    }

    private CatalogClientProducer producer;
    private FloeConfig config;
    private FloeConfig.Catalog catalogConfig;
    private FloeConfig.Catalog.S3 s3Config;
    private FloeConfig.Catalog.Hive hiveConfig;
    private FloeConfig.Catalog.Polaris polarisConfig;
    private FloeConfig.Catalog.Nessie nessieConfig;
    private FloeConfig.Catalog.Lakekeeper lakekeeperConfig;
    private FloeConfig.Catalog.Gravitino gravitinoConfig;
    private CatalogClient createdClient;

    @BeforeEach
    void setUp() {
        producer = new CatalogClientProducer();
        config = mock(FloeConfig.class);
        catalogConfig = mock(FloeConfig.Catalog.class);
        s3Config = mock(FloeConfig.Catalog.S3.class);
        hiveConfig = mock(FloeConfig.Catalog.Hive.class);
        polarisConfig = mock(FloeConfig.Catalog.Polaris.class);
        nessieConfig = mock(FloeConfig.Catalog.Nessie.class);
        lakekeeperConfig = mock(FloeConfig.Catalog.Lakekeeper.class);
        gravitinoConfig = mock(FloeConfig.Catalog.Gravitino.class);

        when(config.catalog()).thenReturn(catalogConfig);
        when(catalogConfig.s3()).thenReturn(s3Config);
        when(catalogConfig.hive()).thenReturn(hiveConfig);
        when(catalogConfig.polaris()).thenReturn(polarisConfig);
        when(catalogConfig.nessie()).thenReturn(nessieConfig);
        when(catalogConfig.lakekeeper()).thenReturn(lakekeeperConfig);
        when(catalogConfig.gravitino()).thenReturn(gravitinoConfig);

        // Default S3 config (no endpoint)
        when(s3Config.endpoint()).thenReturn(Optional.empty());
        when(s3Config.accessKeyId()).thenReturn("");
        when(s3Config.secretAccessKey()).thenReturn("");
        when(s3Config.region()).thenReturn("us-east-1");

        // Inject the config
        producer.config = config;
        createdClient = null;
    }

    @AfterEach
    void closeClient() {
        // Ensure catalog clients are properly closed to avoid resource leak warnings
        if (createdClient != null) {
            createdClient.close();
        }
    }

    private CatalogClient createAndTrackClient() {
        createdClient = producer.produceCatalogClient();
        return createdClient;
    }

    @Nested
    @DisplayName("REST catalog")
    class RestCatalogTests {

        @BeforeEach
        void setUp() {
            when(catalogConfig.type()).thenReturn("REST");
            when(catalogConfig.name()).thenReturn("demo");
            when(catalogConfig.uri()).thenReturn("http://localhost:8181");
            when(catalogConfig.warehouse()).thenReturn("s3://warehouse/");
        }

        @Test
        @DisplayName("creates REST catalog client with basic config")
        void createsRestCatalogClient() {
            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("demo");
        }

        @Test
        @DisplayName("creates REST catalog client with S3 endpoint")
        void createsRestCatalogClientWithS3Endpoint() {
            when(s3Config.endpoint()).thenReturn(Optional.of("http://minio:9000"));
            when(s3Config.accessKeyId()).thenReturn("admin");
            when(s3Config.secretAccessKey()).thenReturn("password");

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("demo");
        }

        @Test
        @DisplayName("handles lowercase type")
        void handlesLowercaseType() {
            when(catalogConfig.type()).thenReturn("rest");

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("demo");
        }
    }

    @Nested
    @DisplayName("Hive catalog")
    class HiveCatalogTests {

        @BeforeEach
        void setUp() {
            when(catalogConfig.type()).thenReturn("HIVE");
            when(catalogConfig.name()).thenReturn("hive-catalog");
            when(catalogConfig.warehouse()).thenReturn("s3://warehouse/");
        }

        @Test
        @DisplayName("creates Hive catalog client with metastore URI")
        void createsHiveCatalogClient() {
            when(hiveConfig.uri()).thenReturn(Optional.of("thrift://localhost:9083"));

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("hive-catalog");
        }

        @Test
        @DisplayName("returns stub client when metastore URI is missing")
        void returnsStubClientWhenMissingUri() {
            when(hiveConfig.uri()).thenReturn(Optional.empty());

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("hive-catalog");
            // Stub client returns false for isHealthy
            assertThat(client.isHealthy()).isFalse();
        }

        // Note: S3 config with Hive is tested in integration tests only,
        // as it creates real S3FileIO instances that require proper cleanup.
    }

    @Nested
    @DisplayName("Polaris catalog")
    class PolarisCatalogTests {

        @BeforeEach
        void setUp() {
            when(catalogConfig.type()).thenReturn("POLARIS");
            when(catalogConfig.name()).thenReturn("polaris-catalog");
            when(catalogConfig.warehouse()).thenReturn("s3://warehouse/");
        }

        @Test
        @DisplayName("creates Polaris catalog client with OAuth2 config")
        void createsPolarisCatalogClient() {
            when(polarisConfig.uri()).thenReturn(Optional.of("https://polaris.example.com"));
            when(polarisConfig.clientId()).thenReturn(Optional.of("floe-client"));
            when(polarisConfig.clientSecret()).thenReturn(Optional.of("secret"));
            when(polarisConfig.principalRole()).thenReturn(Optional.empty());

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("polaris-catalog");
        }

        @Test
        @DisplayName("creates Polaris catalog client with principal role")
        void createsPolarisCatalogClientWithPrincipalRole() {
            when(polarisConfig.uri()).thenReturn(Optional.of("https://polaris.example.com"));
            when(polarisConfig.clientId()).thenReturn(Optional.of("floe-client"));
            when(polarisConfig.clientSecret()).thenReturn(Optional.of("secret"));
            when(polarisConfig.principalRole()).thenReturn(Optional.of("catalog_admin"));

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("polaris-catalog");
        }

        @Test
        @DisplayName("returns stub client when URI is missing")
        void returnsStubClientWhenMissingUri() {
            when(polarisConfig.uri()).thenReturn(Optional.empty());
            when(polarisConfig.clientId()).thenReturn(Optional.of("client"));
            when(polarisConfig.clientSecret()).thenReturn(Optional.of("secret"));

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.isHealthy()).isFalse();
        }

        @Test
        @DisplayName("returns stub client when client ID is missing")
        void returnsStubClientWhenMissingClientId() {
            when(polarisConfig.uri()).thenReturn(Optional.of("https://polaris.example.com"));
            when(polarisConfig.clientId()).thenReturn(Optional.empty());
            when(polarisConfig.clientSecret()).thenReturn(Optional.of("secret"));

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.isHealthy()).isFalse();
        }

        @Test
        @DisplayName("returns stub client when client secret is missing")
        void returnsStubClientWhenMissingClientSecret() {
            when(polarisConfig.uri()).thenReturn(Optional.of("https://polaris.example.com"));
            when(polarisConfig.clientId()).thenReturn(Optional.of("client"));
            when(polarisConfig.clientSecret()).thenReturn(Optional.empty());

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.isHealthy()).isFalse();
        }
    }

    @Nested
    @DisplayName("Nessie catalog")
    class NessieCatalogTests {

        @BeforeEach
        void setUp() {
            when(catalogConfig.type()).thenReturn("NESSIE");
            when(catalogConfig.name()).thenReturn("nessie-catalog");
            when(catalogConfig.warehouse()).thenReturn("s3://warehouse/");
        }

        @Test
        @DisplayName("creates Nessie catalog client with basic config")
        void createsNessieCatalogClient() {
            when(nessieConfig.uri()).thenReturn(Optional.of("https://nessie.example.com/api/v1"));
            when(nessieConfig.ref()).thenReturn("main");
            when(nessieConfig.token()).thenReturn(Optional.empty());

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("nessie-catalog");
        }

        // Note: Bearer token auth with Nessie is tested in integration tests only,
        // as it triggers Iceberg OAuth2 warnings that print to stderr.

        @Test
        @DisplayName("creates Nessie catalog client with custom ref")
        void createsNessieCatalogClientWithCustomRef() {
            when(nessieConfig.uri()).thenReturn(Optional.of("https://nessie.example.com/api/v1"));
            when(nessieConfig.ref()).thenReturn("feature/my-branch");
            when(nessieConfig.token()).thenReturn(Optional.empty());

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("nessie-catalog");
        }

        @Test
        @DisplayName("returns stub client when URI is missing")
        void returnsStubClientWhenMissingUri() {
            when(nessieConfig.uri()).thenReturn(Optional.empty());
            when(nessieConfig.ref()).thenReturn("main");
            when(nessieConfig.token()).thenReturn(Optional.empty());

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.isHealthy()).isFalse();
        }
    }

    @Nested
    @DisplayName("Lakekeeper catalog")
    class LakekeeperCatalogTests {

        @BeforeEach
        void setUp() {
            when(catalogConfig.type()).thenReturn("LAKEKEEPER");
            when(catalogConfig.name()).thenReturn("lakekeeper-catalog");
            when(catalogConfig.warehouse()).thenReturn("s3://warehouse/");
        }

        @Test
        @DisplayName("creates Lakekeeper catalog client with basic config")
        void createsLakekeeperCatalogClient() {
            when(lakekeeperConfig.uri())
                    .thenReturn(Optional.of("https://lakekeeper.example.com/catalog"));
            when(lakekeeperConfig.credential()).thenReturn(Optional.empty());
            when(lakekeeperConfig.oauth2ServerUri()).thenReturn(Optional.empty());
            when(lakekeeperConfig.scope()).thenReturn(Optional.empty());
            when(lakekeeperConfig.nestedNamespaceEnabled()).thenReturn(true);
            when(lakekeeperConfig.vendedCredentialsEnabled()).thenReturn(true);

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("lakekeeper-catalog");
        }

        @Test
        @DisplayName("creates Lakekeeper catalog client with OAuth2 credentials")
        void createsLakekeeperCatalogClientWithOAuth2() {
            when(lakekeeperConfig.uri())
                    .thenReturn(Optional.of("https://lakekeeper.example.com/catalog"));
            when(lakekeeperConfig.credential()).thenReturn(Optional.of("client-id:client-secret"));
            when(lakekeeperConfig.oauth2ServerUri())
                    .thenReturn(Optional.of("https://idp.example.com/oauth/token"));
            when(lakekeeperConfig.scope()).thenReturn(Optional.of("catalog:read catalog:write"));
            when(lakekeeperConfig.nestedNamespaceEnabled()).thenReturn(true);
            when(lakekeeperConfig.vendedCredentialsEnabled()).thenReturn(true);

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("lakekeeper-catalog");
        }

        @Test
        @DisplayName("creates Lakekeeper catalog client with S3 endpoint")
        void createsLakekeeperCatalogClientWithS3Endpoint() {
            when(lakekeeperConfig.uri())
                    .thenReturn(Optional.of("https://lakekeeper.example.com/catalog"));
            when(lakekeeperConfig.credential()).thenReturn(Optional.empty());
            when(lakekeeperConfig.oauth2ServerUri()).thenReturn(Optional.empty());
            when(lakekeeperConfig.scope()).thenReturn(Optional.empty());
            when(lakekeeperConfig.nestedNamespaceEnabled()).thenReturn(true);
            when(lakekeeperConfig.vendedCredentialsEnabled()).thenReturn(true);

            when(s3Config.endpoint()).thenReturn(Optional.of("http://minio:9000"));
            when(s3Config.accessKeyId()).thenReturn("admin");
            when(s3Config.secretAccessKey()).thenReturn("password");

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("lakekeeper-catalog");
        }

        @Test
        @DisplayName("handles lowercase type")
        void handlesLowercaseType() {
            when(catalogConfig.type()).thenReturn("lakekeeper");
            when(lakekeeperConfig.uri())
                    .thenReturn(Optional.of("https://lakekeeper.example.com/catalog"));
            when(lakekeeperConfig.credential()).thenReturn(Optional.empty());
            when(lakekeeperConfig.oauth2ServerUri()).thenReturn(Optional.empty());
            when(lakekeeperConfig.scope()).thenReturn(Optional.empty());
            when(lakekeeperConfig.nestedNamespaceEnabled()).thenReturn(true);
            when(lakekeeperConfig.vendedCredentialsEnabled()).thenReturn(true);

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("lakekeeper-catalog");
        }

        @Test
        @DisplayName("returns stub client when URI is missing")
        void returnsStubClientWhenMissingUri() {
            when(lakekeeperConfig.uri()).thenReturn(Optional.empty());
            when(lakekeeperConfig.credential()).thenReturn(Optional.empty());
            when(lakekeeperConfig.oauth2ServerUri()).thenReturn(Optional.empty());
            when(lakekeeperConfig.scope()).thenReturn(Optional.empty());

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.isHealthy()).isFalse();
        }

        @Test
        @DisplayName("uses default OAuth2 token endpoint when not specified")
        void usesDefaultOAuth2TokenEndpoint() {
            when(lakekeeperConfig.uri())
                    .thenReturn(Optional.of("https://lakekeeper.example.com/catalog"));
            when(lakekeeperConfig.credential()).thenReturn(Optional.of("client-id:client-secret"));
            when(lakekeeperConfig.oauth2ServerUri()).thenReturn(Optional.empty());
            when(lakekeeperConfig.scope()).thenReturn(Optional.empty());
            when(lakekeeperConfig.nestedNamespaceEnabled()).thenReturn(true);
            when(lakekeeperConfig.vendedCredentialsEnabled()).thenReturn(true);

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("lakekeeper-catalog");
        }

        @Test
        @DisplayName("ignores invalid credential format")
        void ignoresInvalidCredentialFormat() {
            when(lakekeeperConfig.uri())
                    .thenReturn(Optional.of("https://lakekeeper.example.com/catalog"));
            when(lakekeeperConfig.credential()).thenReturn(Optional.of("invalid-no-colon"));
            when(lakekeeperConfig.oauth2ServerUri()).thenReturn(Optional.empty());
            when(lakekeeperConfig.scope()).thenReturn(Optional.empty());
            when(lakekeeperConfig.nestedNamespaceEnabled()).thenReturn(true);
            when(lakekeeperConfig.vendedCredentialsEnabled()).thenReturn(true);

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("lakekeeper-catalog");
        }
    }

    @Nested
    @DisplayName("Gravitino catalog")
    class GravitinoCatalogTests {

        @BeforeEach
        void setUp() {
            when(catalogConfig.type()).thenReturn("GRAVITINO");
            when(catalogConfig.name()).thenReturn("gravitino-catalog");
            when(catalogConfig.warehouse()).thenReturn("s3://warehouse/");
        }

        @Test
        @DisplayName("creates Gravitino catalog client with basic config")
        void createsGravitinoCatalogClient() {
            when(gravitinoConfig.uri())
                    .thenReturn(Optional.of("http://gravitino:9001/iceberg/demo"));
            when(gravitinoConfig.credential()).thenReturn(Optional.empty());
            when(gravitinoConfig.oauth2ServerUri()).thenReturn(Optional.empty());
            when(gravitinoConfig.metalake()).thenReturn(Optional.empty());
            when(gravitinoConfig.vendedCredentialsEnabled()).thenReturn(true);

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("gravitino-catalog");
        }

        @Test
        @DisplayName("creates Gravitino catalog client with OAuth2 credentials")
        void createsGravitinoCatalogClientWithOAuth2() {
            when(gravitinoConfig.uri())
                    .thenReturn(Optional.of("http://gravitino:9001/iceberg/demo"));
            when(gravitinoConfig.credential()).thenReturn(Optional.of("client-id:client-secret"));
            when(gravitinoConfig.oauth2ServerUri())
                    .thenReturn(Optional.of("https://idp.example.com/oauth/token"));
            when(gravitinoConfig.metalake()).thenReturn(Optional.of("demo"));
            when(gravitinoConfig.vendedCredentialsEnabled()).thenReturn(true);

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("gravitino-catalog");
        }

        @Test
        @DisplayName("creates Gravitino catalog client with S3 endpoint")
        void createsGravitinoCatalogClientWithS3Endpoint() {
            when(gravitinoConfig.uri())
                    .thenReturn(Optional.of("http://gravitino:9001/iceberg/demo"));
            when(gravitinoConfig.credential()).thenReturn(Optional.empty());
            when(gravitinoConfig.oauth2ServerUri()).thenReturn(Optional.empty());
            when(gravitinoConfig.metalake()).thenReturn(Optional.empty());
            when(gravitinoConfig.vendedCredentialsEnabled()).thenReturn(true);

            when(s3Config.endpoint()).thenReturn(Optional.of("http://minio:9000"));
            when(s3Config.accessKeyId()).thenReturn("admin");
            when(s3Config.secretAccessKey()).thenReturn("password");

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("gravitino-catalog");
        }

        @Test
        @DisplayName("handles lowercase type")
        void handlesLowercaseType() {
            when(catalogConfig.type()).thenReturn("gravitino");
            when(gravitinoConfig.uri())
                    .thenReturn(Optional.of("http://gravitino:9001/iceberg/demo"));
            when(gravitinoConfig.credential()).thenReturn(Optional.empty());
            when(gravitinoConfig.oauth2ServerUri()).thenReturn(Optional.empty());
            when(gravitinoConfig.metalake()).thenReturn(Optional.empty());
            when(gravitinoConfig.vendedCredentialsEnabled()).thenReturn(true);

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("gravitino-catalog");
        }

        @Test
        @DisplayName("returns stub client when URI is missing")
        void returnsStubClientWhenMissingUri() {
            when(gravitinoConfig.uri()).thenReturn(Optional.empty());
            when(gravitinoConfig.credential()).thenReturn(Optional.empty());
            when(gravitinoConfig.oauth2ServerUri()).thenReturn(Optional.empty());
            when(gravitinoConfig.metalake()).thenReturn(Optional.empty());

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.isHealthy()).isFalse();
        }
    }

    @Nested
    @DisplayName("Unsupported catalog type")
    class UnsupportedCatalogTypeTests {

        @Test
        @DisplayName("returns stub client for unknown catalog type")
        void returnsStubClientForUnknownType() {
            when(catalogConfig.type()).thenReturn("UNKNOWN");
            when(catalogConfig.name()).thenReturn("unknown-catalog");
            when(catalogConfig.warehouse()).thenReturn("s3://warehouse/");

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("unknown-catalog");
            assertThat(client.isHealthy()).isFalse();
            assertThat(client.listNamespaces()).isEmpty();
            assertThat(client.listAllTables()).isEmpty();
        }
    }

    @Nested
    @DisplayName("StubCatalogClient")
    class StubCatalogClientTests {

        @Test
        @DisplayName("stub client returns empty results for all operations")
        void stubClientReturnsEmptyResults() {
            when(catalogConfig.type()).thenReturn("INVALID");
            when(catalogConfig.name()).thenReturn("stub-test");
            when(catalogConfig.warehouse()).thenReturn("s3://warehouse/");

            CatalogClient client = createAndTrackClient();

            assertThat(client.getCatalogName()).isEqualTo("stub-test");
            assertThat(client.isHealthy()).isFalse();
            assertThat(client.listNamespaces()).isEmpty();
            assertThat(client.listTables("any")).isEmpty();
            assertThat(client.listAllTables()).isEmpty();
            assertThat(client.loadTable(null)).isEmpty();
            assertThat(client.getTableMetadata(null)).isEmpty();

            // close should not throw
            client.close();
        }
    }

    @Nested
    @DisplayName("S3 configuration")
    class S3ConfigurationTests {

        @BeforeEach
        void setUp() {
            when(catalogConfig.type()).thenReturn("REST");
            when(catalogConfig.name()).thenReturn("s3-test");
            when(catalogConfig.uri()).thenReturn("http://localhost:8181");
            when(catalogConfig.warehouse()).thenReturn("s3://warehouse/");
        }

        @Test
        @DisplayName("skips S3 config when endpoint is empty")
        void skipsS3ConfigWhenEndpointEmpty() {
            when(s3Config.endpoint()).thenReturn(Optional.of(""));

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
        }

        @Test
        @DisplayName("skips S3 config when endpoint is blank")
        void skipsS3ConfigWhenEndpointBlank() {
            when(s3Config.endpoint()).thenReturn(Optional.of("   "));

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
        }

        @Test
        @DisplayName("applies S3 config when endpoint is present")
        void appliesS3ConfigWhenEndpointPresent() {
            when(s3Config.endpoint()).thenReturn(Optional.of("http://minio:9000"));
            when(s3Config.accessKeyId()).thenReturn("admin");
            when(s3Config.secretAccessKey()).thenReturn("password");
            when(s3Config.region()).thenReturn("us-west-2");

            CatalogClient client = createAndTrackClient();

            assertThat(client).isNotNull();
            assertThat(client.getCatalogName()).isEqualTo("s3-test");
        }
    }
}
