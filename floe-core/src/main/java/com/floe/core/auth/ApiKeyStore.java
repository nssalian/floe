package com.floe.core.auth;

import java.util.List;
import java.util.Optional;

/**
 * Storage abstraction for API keys.
 *
 * <p>Implementations can persist keys to various backends: - In-memory (for testing and simple
 * deployments) - Database (for production)
 */
public interface ApiKeyStore {

    /**
     * Find an API key by its hash.
     *
     * @param keyHash The SHA-256 hash of the key
     * @return The API key if found
     */
    Optional<ApiKey> findByKeyHash(String keyHash);

    /**
     * Find an API key by its ID.
     *
     * @param id The key ID
     * @return The API key if found
     */
    Optional<ApiKey> findById(String id);

    /**
     * Find an API key by its name.
     *
     * @param name The key name
     * @return The API key if found
     */
    Optional<ApiKey> findByName(String name);

    /**
     * List all API keys.
     *
     * @return All stored keys
     */
    List<ApiKey> listAll();

    /**
     * List all enabled API keys.
     *
     * @return All enabled keys
     */
    List<ApiKey> listEnabled();

    /**
     * Save an API key.
     *
     * @param apiKey The key to save
     */
    void save(ApiKey apiKey);

    /**
     * Update the last used timestamp for a key.
     *
     * @param id The key ID
     */
    void updateLastUsed(String id);

    /**
     * Delete an API key by ID.
     *
     * @param id The key ID
     * @return true if the key was deleted
     */
    boolean deleteById(String id);

    /**
     * Check if a key exists by name.
     *
     * @param name The key name
     * @return true if a key with this name exists
     */
    boolean existsByName(String name);

    /**
     * Check if a key exists by ID.
     *
     * @param id The key ID
     * @return true if a key with this ID exists
     */
    boolean existsById(String id);

    /** Count total keys in the store. */
    long count();

    /** Clear all keys. For testing only. */
    void clear();
}
