package com.floe.core.auth;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("InMemoryApiKeyStore")
class InMemoryApiKeyStoreTest {

    private InMemoryApiKeyStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryApiKeyStore();
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
        @DisplayName("should update existing key")
        void shouldUpdateExistingKey() {
            ApiKey original = createTestKey("key-1", "test-key", "hash-1");
            store.save(original);

            ApiKey updated = original.withRole(Role.ADMIN);
            store.save(updated);

            assertEquals(1, store.count());
            Optional<ApiKey> found = store.findById("key-1");
            assertTrue(found.isPresent());
            assertEquals(Role.ADMIN, found.get().role());
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
        @DisplayName("should allow renaming own key")
        void shouldAllowRenamingOwnKey() {
            ApiKey original = createTestKey("key-1", "original-name", "hash-1");
            store.save(original);

            ApiKey renamed = original.withName("new-name");
            store.save(renamed);

            assertEquals(1, store.count());
            assertTrue(store.findByName("new-name").isPresent());
            assertFalse(store.findByName("original-name").isPresent());
        }

        @Test
        @DisplayName("should update indexes on save")
        void shouldUpdateIndexesOnSave() {
            ApiKey original = createTestKey("key-1", "name-1", "hash-1");
            store.save(original);

            ApiKey updated =
                    ApiKey.builder()
                            .id("key-1")
                            .keyHash("hash-2")
                            .name("name-2")
                            .role(Role.ADMIN)
                            .build();
            store.save(updated);

            assertFalse(store.findByKeyHash("hash-1").isPresent());
            assertFalse(store.findByName("name-1").isPresent());
            assertTrue(store.findByKeyHash("hash-2").isPresent());
            assertTrue(store.findByName("name-2").isPresent());
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
        @DisplayName("should return immutable copy")
        void listAllShouldReturnImmutableCopy() {
            store.save(createTestKey("key-1", "key-a", "hash-1"));

            List<ApiKey> all = store.listAll();

            assertThrows(
                    UnsupportedOperationException.class,
                    () -> all.add(createTestKey("key-2", "key-b", "hash-2")));
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
            store.updateLastUsed("nonexistent");
        }

        @Test
        @DisplayName("should update all indexes")
        void updateLastUsedShouldUpdateAllIndexes() {
            ApiKey key = createTestKey("key-1", "test-key", "hash-1");
            store.save(key);

            store.updateLastUsed("key-1");

            Instant lastUsed = store.findById("key-1").get().lastUsedAt();

            assertEquals(lastUsed, store.findByKeyHash("hash-1").get().lastUsedAt());
            assertEquals(lastUsed, store.findByName("test-key").get().lastUsedAt());
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

        @Test
        @DisplayName("should remove from all indexes")
        void deleteShouldRemoveFromAllIndexes() {
            ApiKey key = createTestKey("key-1", "test-key", "hash-1");
            store.save(key);

            store.deleteById("key-1");

            assertFalse(store.findById("key-1").isPresent());
            assertFalse(store.findByKeyHash("hash-1").isPresent());
            assertFalse(store.findByName("test-key").isPresent());
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

        @Test
        @DisplayName("should remove from all indexes")
        void clearShouldRemoveFromAllIndexes() {
            store.save(createTestKey("key-1", "test-key", "hash-1"));

            store.clear();

            assertFalse(store.findById("key-1").isPresent());
            assertFalse(store.findByKeyHash("hash-1").isPresent());
            assertFalse(store.findByName("test-key").isPresent());
        }
    }

    @Nested
    @DisplayName("Concurrency")
    class Concurrency {

        @Test
        @DisplayName("should handle concurrent access")
        void shouldHandleConcurrentAccess() throws InterruptedException {
            int numThreads = 10;
            Thread[] threads = new Thread[numThreads];

            for (int i = 0; i < numThreads; i++) {
                final int idx = i;
                threads[i] =
                        new Thread(
                                () ->
                                        store.save(
                                                createTestKey(
                                                        "key-" + idx,
                                                        "name-" + idx,
                                                        "hash-" + idx)));
            }

            for (Thread thread : threads) {
                thread.start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            assertEquals(numThreads, store.count());
        }
    }
}
