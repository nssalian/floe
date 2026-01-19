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

        when(config.catalog()).thenReturn(catalogConfig);
        when(catalogConfig.s3()).thenReturn(s3Config);
        when(catalogConfig.hive()).thenReturn(hiveConfig);
        when(catalogConfig.polaris()).thenReturn(polarisConfig);
        when(catalogConfig.nessie()).thenReturn(nessieConfig);

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
