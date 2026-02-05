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

import java.util.Optional;

/** Store for persisting catalog configuration. Only one active config is allowed at a time. */
public interface CatalogConfigStore {

    /**
     * Saves or updates the catalog config and sets it as active.
     *
     * @param config The catalog config to save
     * @return The saved config (with generated ID if new)
     */
    CatalogConfig upsertAndActivate(CatalogConfig config);

    /**
     * Finds the currently active catalog config.
     *
     * @return The active config, or empty if none
     */
    Optional<CatalogConfig> findActive();

    /**
     * Finds a catalog config by name and type.
     *
     * @param name Catalog name
     * @param type Catalog type
     * @return The config, or empty if not found
     */
    Optional<CatalogConfig> findByNameAndType(String name, String type);
}
