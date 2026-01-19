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
