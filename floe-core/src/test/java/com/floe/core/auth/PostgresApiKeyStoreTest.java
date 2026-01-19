package com.floe.core.auth;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration")
@Testcontainers
@DisplayName("PostgresApiKeyStore")
class PostgresApiKeyStoreTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:15-alpine")
                    .withDatabaseName("floe_test")
                    .withUsername("test")
                    .withPassword("test");

    private PostgresApiKeyStore store;

    @BeforeEach
    void setUp() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());

        store = new PostgresApiKeyStore(dataSource);
        store.initializeSchema();
        store.clear();
    }

    private ApiKey createTestKey(String id, String name, String hash) {
        return ApiKey.builder()
                .id(id)
                .keyHash(hash)
                .name(name)
                .role(Role.VIEWER)
                .enabled(true)
                .build();
    }

    @Nested
    @DisplayName("initializeSchema")
    class InitializeSchema {

        @Test
        @DisplayName("should initialize schema without error")
        void shouldInitializeSchemaWithoutError() {
            assertDoesNotThrow(() -> store.initializeSchema());
        }
    }

    @Nested
    @DisplayName("save")
    class Save {

        @Test
        @DisplayName("should save new key")
        void shouldSaveNewKey() {
            ApiKey key = createTestKey("key-1", "test-key", "hash-1");

            store.save(key);

            assertEquals(1, store.count());
            assertTrue(store.findById("key-1").isPresent());
        }

        @Test
        @DisplayName("should save key with all fields")
        void shouldSaveKeyWithAllFields() {
            Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            Instant expiresAt = now.plus(30, ChronoUnit.DAYS);

            ApiKey key =
                    ApiKey.builder()
                            .id("key-full")
                            .keyHash("hash-full")
                            .name("full-key")
                            .role(Role.ADMIN)
                            .enabled(true)
                            .createdAt(now)
                            .expiresAt(expiresAt)
                            .lastUsedAt(now)
                            .createdBy("admin-user")
                            .build();

            store.save(key);

            Optional<ApiKey> found = store.findById("key-full");
            assertTrue(found.isPresent());

            ApiKey saved = found.get();
            assertEquals("key-full", saved.id());
            assertEquals("hash-full", saved.keyHash());
            assertEquals("full-key", saved.name());
            assertEquals(Role.ADMIN, saved.role());
            assertTrue(saved.enabled());
            assertEquals(now, saved.createdAt());
            assertEquals(expiresAt, saved.expiresAt());
            assertEquals(now, saved.lastUsedAt());
            assertEquals("admin-user", saved.createdBy());
        }

        @Test
        @DisplayName("should update existing key")
        void shouldUpdateExistingKey() {
            ApiKey original = createTestKey("key-1", "test-key", "hash-1");
            store.save(original);

            ApiKey updated = original.withRole(Role.ADMIN).withEnabled(false);
            store.save(updated);

            assertEquals(1, store.count());
            Optional<ApiKey> found = store.findById("key-1");
            assertTrue(found.isPresent());
            assertEquals(Role.ADMIN, found.get().role());
            assertFalse(found.get().enabled());
        }

        @Test
        @DisplayName("should throw when saving duplicate name")
        void shouldThrowWhenSavingDuplicateName() {
            ApiKey key1 = createTestKey("key-1", "same-name", "hash-1");
            ApiKey key2 = createTestKey("key-2", "same-name", "hash-2");

            store.save(key1);

            IllegalArgumentException ex =
                    assertThrows(IllegalArgumentException.class, () -> store.save(key2));

            assertTrue(ex.getMessage().contains("already exists"));
        }

        @Test
        @DisplayName("should save key with null optional fields")
        void shouldSaveKeyWithNullOptionalFields() {
            ApiKey key =
                    ApiKey.builder()
                            .id("key-minimal")
                            .keyHash("hash-minimal")
                            .name("minimal-key")
                            .role(Role.VIEWER)
                            .enabled(true)
                            .expiresAt(null)
                            .lastUsedAt(null)
                            .createdBy(null)
                            .build();

            store.save(key);

            Optional<ApiKey> found = store.findById("key-minimal");
            assertTrue(found.isPresent());
            assertNull(found.get().expiresAt());
            assertNull(found.get().lastUsedAt());
            assertNull(found.get().createdBy());
        }
    }

    @Nested
    @DisplayName("findByKeyHash")
    class FindByKeyHash {

        @Test
        @DisplayName("should find by key hash")
        void shouldFindByKeyHash() {
            ApiKey key = createTestKey("key-1", "test-key", "hash-abc");
            store.save(key);

            Optional<ApiKey> found = store.findByKeyHash("hash-abc");

            assertTrue(found.isPresent());
            assertEquals("key-1", found.get().id());
        }

        @Test
        @DisplayName("should return empty when key hash not found")
        void shouldReturnEmptyWhenKeyHashNotFound() {
            Optional<ApiKey> found = store.findByKeyHash("nonexistent");
            assertTrue(found.isEmpty());
        }
    }

    @Nested
    @DisplayName("findById")
    class FindById {

        @Test
        @DisplayName("should find by id")
        void shouldFindById() {
            ApiKey key = createTestKey("key-123", "test-key", "hash-1");
            store.save(key);

            Optional<ApiKey> found = store.findById("key-123");

            assertTrue(found.isPresent());
            assertEquals("test-key", found.get().name());
        }

        @Test
        @DisplayName("should return empty when id not found")
        void shouldReturnEmptyWhenIdNotFound() {
            Optional<ApiKey> found = store.findById("nonexistent");
            assertTrue(found.isEmpty());
        }
    }

    @Nested
    @DisplayName("findByName")
    class FindByName {

        @Test
        @DisplayName("should find by name")
        void shouldFindByName() {
            ApiKey key = createTestKey("key-1", "my-api-key", "hash-1");
            store.save(key);

            Optional<ApiKey> found = store.findByName("my-api-key");

            assertTrue(found.isPresent());
            assertEquals("key-1", found.get().id());
        }

        @Test
        @DisplayName("should return empty when name not found")
        void shouldReturnEmptyWhenNameNotFound() {
            Optional<ApiKey> found = store.findByName("nonexistent");
            assertTrue(found.isEmpty());
        }
    }

    @Nested
    @DisplayName("listAll")
    class ListAll {

        @Test
        @DisplayName("should list all keys")
        void shouldListAllKeys() {
            store.save(createTestKey("key-1", "key-a", "hash-1"));
            store.save(createTestKey("key-2", "key-b", "hash-2"));
            store.save(createTestKey("key-3", "key-c", "hash-3"));

            List<ApiKey> all = store.listAll();

            assertEquals(3, all.size());
        }

        @Test
        @DisplayName("should return empty list when no keys")
        void shouldReturnEmptyListWhenNoKeys() {
            List<ApiKey> all = store.listAll();
            assertTrue(all.isEmpty());
        }

        @Test
        @DisplayName("should be ordered by createdAt descending")
        void listAllShouldBeOrderedByCreatedAtDescending() {
            Instant now = Instant.now();

            ApiKey oldest =
                    ApiKey.builder()
                            .id("key-1")
                            .keyHash("hash-1")
                            .name("oldest")
                            .role(Role.VIEWER)
                            .createdAt(now.minus(2, ChronoUnit.DAYS))
                            .build();
            ApiKey middle =
                    ApiKey.builder()
                            .id("key-2")
                            .keyHash("hash-2")
                            .name("middle")
                            .role(Role.VIEWER)
                            .createdAt(now.minus(1, ChronoUnit.DAYS))
                            .build();
            ApiKey newest =
                    ApiKey.builder()
                            .id("key-3")
                            .keyHash("hash-3")
                            .name("newest")
                            .role(Role.VIEWER)
                            .createdAt(now)
                            .build();

            store.save(oldest);
            store.save(middle);
            store.save(newest);

            List<ApiKey> all = store.listAll();

            assertEquals("newest", all.get(0).name());
            assertEquals("middle", all.get(1).name());
            assertEquals("oldest", all.get(2).name());
        }
    }

    @Nested
    @DisplayName("listEnabled")
    class ListEnabled {

        @Test
        @DisplayName("should list only enabled keys")
        void shouldListOnlyEnabledKeys() {
            ApiKey enabled1 =
                    ApiKey.builder()
                            .id("key-1")
                            .keyHash("hash-1")
                            .name("enabled-1")
                            .role(Role.VIEWER)
                            .enabled(true)
                            .build();
            ApiKey disabled =
                    ApiKey.builder()
                            .id("key-2")
                            .keyHash("hash-2")
                            .name("disabled")
                            .role(Role.VIEWER)
                            .enabled(false)
                            .build();
            ApiKey enabled2 =
                    ApiKey.builder()
                            .id("key-3")
                            .keyHash("hash-3")
                            .name("enabled-2")
                            .role(Role.VIEWER)
                            .enabled(true)
                            .build();

            store.save(enabled1);
            store.save(disabled);
            store.save(enabled2);

            List<ApiKey> enabled = store.listEnabled();

            assertEquals(2, enabled.size());
            assertTrue(enabled.stream().allMatch(ApiKey::enabled));
        }

        @Test
        @DisplayName("should return empty when no enabled keys")
        void shouldReturnEmptyWhenNoEnabledKeys() {
            ApiKey disabled =
                    ApiKey.builder()
                            .id("key-1")
                            .keyHash("hash-1")
                            .name("disabled")
                            .role(Role.VIEWER)
                            .enabled(false)
                            .build();
            store.save(disabled);

            List<ApiKey> enabled = store.listEnabled();

            assertTrue(enabled.isEmpty());
        }
    }

    @Nested
    @DisplayName("updateLastUsed")
    class UpdateLastUsed {

        @Test
        @DisplayName("should update lastUsedAt")
        void shouldUpdateLastUsedAt() {
            ApiKey key = createTestKey("key-1", "test-key", "hash-1");
            store.save(key);

            assertNull(store.findById("key-1").get().lastUsedAt());

            store.updateLastUsed("key-1");

            ApiKey updated = store.findById("key-1").get();
            assertNotNull(updated.lastUsedAt());
        }

        @Test
        @DisplayName("should not throw when updating nonexistent key")
        void shouldNotThrowWhenUpdatingNonexistentKey() {
            assertDoesNotThrow(() -> store.updateLastUsed("nonexistent"));
        }
    }

    @Nested
    @DisplayName("deleteById")
    class DeleteById {

        @Test
        @DisplayName("should delete by id")
        void shouldDeleteById() {
            ApiKey key = createTestKey("key-1", "test-key", "hash-1");
            store.save(key);

            boolean deleted = store.deleteById("key-1");

            assertTrue(deleted);
            assertEquals(0, store.count());
            assertFalse(store.findById("key-1").isPresent());
        }

        @Test
        @DisplayName("should return false when deleting nonexistent key")
        void shouldReturnFalseWhenDeletingNonexistentKey() {
            boolean deleted = store.deleteById("nonexistent");
            assertFalse(deleted);
        }
    }

    @Nested
    @DisplayName("existsByName")
    class ExistsByName {

        @Test
        @DisplayName("should return true when name exists")
        void shouldReturnTrueWhenNameExists() {
            store.save(createTestKey("key-1", "existing-key", "hash-1"));
            assertTrue(store.existsByName("existing-key"));
        }

        @Test
        @DisplayName("should return false when name does not exist")
        void shouldReturnFalseWhenNameDoesNotExist() {
            assertFalse(store.existsByName("nonexistent"));
        }
    }

    @Nested
    @DisplayName("existsById")
    class ExistsById {

        @Test
        @DisplayName("should return true when id exists")
        void shouldReturnTrueWhenIdExists() {
            store.save(createTestKey("key-123", "test-key", "hash-1"));
            assertTrue(store.existsById("key-123"));
        }

        @Test
        @DisplayName("should return false when id does not exist")
        void shouldReturnFalseWhenIdDoesNotExist() {
            assertFalse(store.existsById("nonexistent"));
        }
    }

    @Nested
    @DisplayName("count")
    class Count {

        @Test
        @DisplayName("should return zero when empty")
        void shouldReturnZeroWhenEmpty() {
            assertEquals(0, store.count());
        }

        @Test
        @DisplayName("should return correct count")
        void shouldReturnCorrectCount() {
            store.save(createTestKey("key-1", "key-a", "hash-1"));
            store.save(createTestKey("key-2", "key-b", "hash-2"));
            assertEquals(2, store.count());
        }
    }

    @Nested
    @DisplayName("clear")
    class Clear {

        @Test
        @DisplayName("should clear all keys")
        void shouldClearAllKeys() {
            store.save(createTestKey("key-1", "key-a", "hash-1"));
            store.save(createTestKey("key-2", "key-b", "hash-2"));
            store.save(createTestKey("key-3", "key-c", "hash-3"));

            store.clear();

            assertEquals(0, store.count());
            assertTrue(store.listAll().isEmpty());
        }
    }

    @Nested
    @DisplayName("Role Mapping")
    class RoleMapping {

        @Test
        @DisplayName("should persist and retrieve all roles")
        void shouldPersistAndRetrieveAllRoles() {
            for (Role role : Role.values()) {
                String id = "key-" + role.name();
                ApiKey key =
                        ApiKey.builder()
                                .id(id)
                                .keyHash("hash-" + role.name())
                                .name("key-" + role.name())
                                .role(role)
                                .build();

                store.save(key);

                Optional<ApiKey> found = store.findById(id);
                assertTrue(found.isPresent());
                assertEquals(role, found.get().role());
            }
        }
    }
}
