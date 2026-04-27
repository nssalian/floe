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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of ApiKeyStore.
 *
 * <p>Suitable for development and testing; data is lost on restart.
 */
public class InMemoryApiKeyStore implements ApiKeyStore {

    private final Object lock = new Object();
    private final Map<String, ApiKey> keysById = new ConcurrentHashMap<>();
    private final Map<String, ApiKey> keysByHash = new ConcurrentHashMap<>();
    private final Map<String, ApiKey> keysByName = new ConcurrentHashMap<>();

    @Override
    public Optional<ApiKey> findByKeyHash(String keyHash) {
        return Optional.ofNullable(keysByHash.get(keyHash));
    }

    @Override
    public Optional<ApiKey> findById(String id) {
        return Optional.ofNullable(keysById.get(id));
    }

    @Override
    public Optional<ApiKey> findByName(String name) {
        return Optional.ofNullable(keysByName.get(name));
    }

    @Override
    public List<ApiKey> listAll() {
        return List.copyOf(keysById.values());
    }

    @Override
    public List<ApiKey> listEnabled() {
        return keysById.values().stream().filter(ApiKey::enabled).toList();
    }

    @Override
    public void save(ApiKey apiKey) {
        synchronized (lock) {
            // Check for duplicate name (different ID)
            ApiKey existingByName = keysByName.get(apiKey.name());
            if (existingByName != null && !existingByName.id().equals(apiKey.id())) {
                throw new IllegalArgumentException(
                        "API key with name '" + apiKey.name() + "' already exists");
            }

            // Remove old entries if updating
            ApiKey existing = keysById.get(apiKey.id());
            if (existing != null) {
                keysByHash.remove(existing.keyHash());
                keysByName.remove(existing.name());
            }

            keysById.put(apiKey.id(), apiKey);
            keysByHash.put(apiKey.keyHash(), apiKey);
            keysByName.put(apiKey.name(), apiKey);
        }
    }

    @Override
    public void updateLastUsed(String id) {
        synchronized (lock) {
            ApiKey existing = keysById.get(id);
            if (existing != null) {
                ApiKey updated = existing.withLastUsedAt(Instant.now());
                keysById.put(id, updated);
                keysByHash.put(updated.keyHash(), updated);
                keysByName.put(updated.name(), updated);
            }
        }
    }

    @Override
    public boolean deleteById(String id) {
        synchronized (lock) {
            ApiKey removed = keysById.remove(id);
            if (removed != null) {
                keysByHash.remove(removed.keyHash());
                keysByName.remove(removed.name());
                return true;
            }
            return false;
        }
    }

    @Override
    public boolean existsByName(String name) {
        return keysByName.containsKey(name);
    }

    @Override
    public boolean existsById(String id) {
        return keysById.containsKey(id);
    }

    @Override
    public long count() {
        return keysById.size();
    }

    @Override
    public void clear() {
        synchronized (lock) {
            keysById.clear();
            keysByHash.clear();
            keysByName.clear();
        }
    }
}
