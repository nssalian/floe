package com.floe.engine.trino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("TrinoEngineConfig")
class TrinoEngineConfigTest {

    private static final String JDBC_URL = "jdbc:trino://localhost:8080";
    private static final String USERNAME = "trino";
    private static final String PASSWORD = "secret";
    private static final String CATALOG = "iceberg";
    private static final String SCHEMA = "default";

    @Nested
    @DisplayName("Builder")
    class Builder {

        @Test
        @DisplayName("should create config with all parameters")
        void shouldCreateWithAllParams() {
            Map<String, String> sessionProps = Map.of("query_max_memory", "1GB");

            TrinoEngineConfig config =
                    TrinoEngineConfig.builder()
                            .jdbcUrl(JDBC_URL)
                            .username(USERNAME)
                            .password(PASSWORD)
                            .catalog(CATALOG)
                            .schema(SCHEMA)
                            .queryTimeoutSeconds(600)
                            .sessionProperties(sessionProps)
                            .build();

            assertThat(config.jdbcUrl()).isEqualTo(JDBC_URL);
            assertThat(config.username()).isEqualTo(USERNAME);
            assertThat(config.password()).isEqualTo(PASSWORD);
            assertThat(config.catalog()).isEqualTo(CATALOG);
            assertThat(config.schema()).isEqualTo(SCHEMA);
            assertThat(config.queryTimeoutSeconds()).isEqualTo(600);
            assertThat(config.sessionProperties()).containsEntry("query_max_memory", "1GB");
        }

        @Test
        @DisplayName("should use default timeout when not specified")
        void shouldUseDefaultTimeout() {
            TrinoEngineConfig config =
                    TrinoEngineConfig.builder()
                            .jdbcUrl(JDBC_URL)
                            .username(USERNAME)
                            .catalog(CATALOG)
                            .build();

            assertThat(config.queryTimeoutSeconds()).isEqualTo(300);
        }

        @Test
        @DisplayName("should handle null password")
        void shouldHandleNullPassword() {
            TrinoEngineConfig config =
                    TrinoEngineConfig.builder()
                            .jdbcUrl(JDBC_URL)
                            .username(USERNAME)
                            .catalog(CATALOG)
                            .password(null)
                            .build();

            assertThat(config.password()).isNull();
        }

        @Test
        @DisplayName("should create empty session properties when not specified")
        void shouldCreateEmptySessionProperties() {
            TrinoEngineConfig config =
                    TrinoEngineConfig.builder()
                            .jdbcUrl(JDBC_URL)
                            .username(USERNAME)
                            .catalog(CATALOG)
                            .build();

            assertThat(config.sessionProperties()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Validation")
    class Validation {

        @Test
        @DisplayName("should throw when jdbcUrl is null")
        void shouldThrowWhenJdbcUrlNull() {
            assertThatThrownBy(
                            () ->
                                    TrinoEngineConfig.builder()
                                            .jdbcUrl(null)
                                            .username(USERNAME)
                                            .catalog(CATALOG)
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("jdbcUrl is required");
        }

        @Test
        @DisplayName("should throw when jdbcUrl is blank")
        void shouldThrowWhenJdbcUrlBlank() {
            assertThatThrownBy(
                            () ->
                                    TrinoEngineConfig.builder()
                                            .jdbcUrl("   ")
                                            .username(USERNAME)
                                            .catalog(CATALOG)
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("jdbcUrl is required");
        }

        @Test
        @DisplayName("should throw when username is null")
        void shouldThrowWhenUsernameNull() {
            assertThatThrownBy(
                            () ->
                                    TrinoEngineConfig.builder()
                                            .jdbcUrl(JDBC_URL)
                                            .username(null)
                                            .catalog(CATALOG)
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("username is required");
        }

        @Test
        @DisplayName("should throw when username is blank")
        void shouldThrowWhenUsernameBlank() {
            assertThatThrownBy(
                            () ->
                                    TrinoEngineConfig.builder()
                                            .jdbcUrl(JDBC_URL)
                                            .username("")
                                            .catalog(CATALOG)
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("username is required");
        }

        @Test
        @DisplayName("should throw when catalog is null")
        void shouldThrowWhenCatalogNull() {
            assertThatThrownBy(
                            () ->
                                    TrinoEngineConfig.builder()
                                            .jdbcUrl(JDBC_URL)
                                            .username(USERNAME)
                                            .catalog(null)
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("catalog is required");
        }

        @Test
        @DisplayName("should throw when catalog is blank")
        void shouldThrowWhenCatalogBlank() {
            assertThatThrownBy(
                            () ->
                                    TrinoEngineConfig.builder()
                                            .jdbcUrl(JDBC_URL)
                                            .username(USERNAME)
                                            .catalog("  ")
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("catalog is required");
        }
    }

    @Nested
    @DisplayName("Immutability")
    class Immutability {

        @Test
        @DisplayName("should return immutable session properties")
        void shouldReturnImmutableSessionProperties() {
            Map<String, String> mutableProps = new java.util.HashMap<>();
            mutableProps.put("key", "value");

            TrinoEngineConfig config =
                    TrinoEngineConfig.builder()
                            .jdbcUrl(JDBC_URL)
                            .username(USERNAME)
                            .catalog(CATALOG)
                            .sessionProperties(mutableProps)
                            .build();

            // Original map modification should not affect config
            mutableProps.put("another", "value");

            assertThat(config.sessionProperties()).hasSize(1);
            assertThat(config.sessionProperties()).containsOnlyKeys("key");

            // Config's map should be immutable
            assertThatThrownBy(() -> config.sessionProperties().put("new", "value"))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    @DisplayName("JDBC URL Formats")
    class JdbcUrlFormats {

        @Test
        @DisplayName("should accept standard Trino JDBC URL")
        void shouldAcceptStandardUrl() {
            TrinoEngineConfig config =
                    TrinoEngineConfig.builder()
                            .jdbcUrl("jdbc:trino://trino-coordinator:8080")
                            .username(USERNAME)
                            .catalog(CATALOG)
                            .build();

            assertThat(config.jdbcUrl()).startsWith("jdbc:trino://");
        }

        @Test
        @DisplayName("should accept Trino JDBC URL with catalog")
        void shouldAcceptUrlWithCatalog() {
            TrinoEngineConfig config =
                    TrinoEngineConfig.builder()
                            .jdbcUrl("jdbc:trino://localhost:8080/iceberg")
                            .username(USERNAME)
                            .catalog(CATALOG)
                            .build();

            assertThat(config.jdbcUrl()).contains("/iceberg");
        }

        @Test
        @DisplayName("should accept Trino JDBC URL with catalog and schema")
        void shouldAcceptUrlWithCatalogAndSchema() {
            TrinoEngineConfig config =
                    TrinoEngineConfig.builder()
                            .jdbcUrl("jdbc:trino://localhost:8080/iceberg/default")
                            .username(USERNAME)
                            .catalog(CATALOG)
                            .build();

            assertThat(config.jdbcUrl()).contains("/iceberg/default");
        }

        @Test
        @DisplayName("should accept Trino JDBC URL with SSL")
        void shouldAcceptUrlWithSsl() {
            TrinoEngineConfig config =
                    TrinoEngineConfig.builder()
                            .jdbcUrl("jdbc:trino://localhost:8443?SSL=true")
                            .username(USERNAME)
                            .catalog(CATALOG)
                            .build();

            assertThat(config.jdbcUrl()).contains("SSL=true");
        }
    }
}
