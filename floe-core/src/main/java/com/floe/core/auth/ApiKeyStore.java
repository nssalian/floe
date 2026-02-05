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
