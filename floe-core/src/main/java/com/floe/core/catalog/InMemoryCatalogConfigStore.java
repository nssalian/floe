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

package com.floe.core.catalog;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of CatalogConfigStore.
 *
 * <p>Useful for testing and development.
 */
public class InMemoryCatalogConfigStore implements CatalogConfigStore {

    private final Map<String, CatalogConfig> configs = new ConcurrentHashMap<>();
    private volatile CatalogConfig activeConfig = null;

    @Override
    public synchronized CatalogConfig upsertAndActivate(CatalogConfig config) {
        String key = config.name() + ":" + config.type();

        // Deactivate current
        if (activeConfig != null) {
            String activeKey = activeConfig.name() + ":" + activeConfig.type();
            configs.put(activeKey, activeConfig.withActive(false));
        }

        // Check if exists
        CatalogConfig existing = configs.get(key);
        CatalogConfig toSave;

        if (existing != null) {
            // Update existing
            toSave =
                    existing.withUpdates(config.uri(), config.warehouse(), config.properties())
                            .withActive(true);
        } else {
            // Create new
            toSave =
                    CatalogConfig.create(
                            config.name(),
                            config.type(),
                            config.uri(),
                            config.warehouse(),
                            config.properties());
        }

        configs.put(key, toSave);
        activeConfig = toSave;
        return toSave;
    }

    @Override
    public Optional<CatalogConfig> findActive() {
        return Optional.ofNullable(activeConfig);
    }

    @Override
    public Optional<CatalogConfig> findByNameAndType(String name, String type) {
        return Optional.ofNullable(configs.get(name + ":" + type));
    }

    /** Clears all stored configs. Useful for testing. */
    public synchronized void clear() {
        configs.clear();
        activeConfig = null;
    }
}
