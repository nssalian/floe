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

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;

/**
 * CatalogClient implementation for Hive Metastore.
 *
 * <p>Connects to a Hive Metastore service via Thrift protocol. Supports both standalone Hive
 * Metastore and embedded metastore configurations.
 */
public class HiveMetastoreCatalogClient extends AbstractIcebergCatalogClient {

    private final HiveCatalog catalog;

    /**
     * Create a Hive Metastore catalog client.
     *
     * @param catalogName the catalog name
     * @param metastoreUri the Hive Metastore Thrift URI (e.g., thrift://localhost:9083)
     * @param warehouse the warehouse location for new tables
     */
    public HiveMetastoreCatalogClient(String catalogName, String metastoreUri, String warehouse) {
        this(catalogName, metastoreUri, warehouse, new HashMap<>());
    }

    /**
     * Create a Hive Metastore catalog client with additional properties.
     *
     * @param catalogName the catalog name
     * @param metastoreUri the Hive Metastore Thrift URI
     * @param warehouse the warehouse location
     * @param additionalProperties additional catalog/Hadoop properties
     */
    public HiveMetastoreCatalogClient(
            String catalogName,
            String metastoreUri,
            String warehouse,
            Map<String, String> additionalProperties) {
        super(catalogName);
        Configuration hadoopConf = new Configuration();

        // Set Hive Metastore URI
        hadoopConf.set("hive.metastore.uris", metastoreUri);

        // Apply any additional Hadoop configuration
        for (Map.Entry<String, String> entry : additionalProperties.entrySet()) {
            if (entry.getKey().startsWith("hadoop.") || entry.getKey().startsWith("hive.")) {
                hadoopConf.set(entry.getKey(), entry.getValue());
            }
        }

        this.catalog = new HiveCatalog();

        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.URI, metastoreUri);
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);

        // Add non-Hadoop properties
        for (Map.Entry<String, String> entry : additionalProperties.entrySet()) {
            if (!entry.getKey().startsWith("hadoop.") && !entry.getKey().startsWith("hive.")) {
                properties.put(entry.getKey(), entry.getValue());
            }
        }

        catalog.setConf(hadoopConf);
        catalog.initialize(catalogName, properties);

        LOG.info(
                "Initialized Hive Metastore catalog '{}' at {} with warehouse {}",
                catalogName,
                metastoreUri,
                warehouse);
    }

    /**
     * Create a Hive Metastore catalog client with a custom Hadoop configuration.
     *
     * @param catalogName the catalog name
     * @param hadoopConf pre-configured Hadoop Configuration (e.g., loaded from hive-site.xml)
     * @param warehouse the warehouse location
     */
    public HiveMetastoreCatalogClient(
            String catalogName, Configuration hadoopConf, String warehouse) {
        super(catalogName);
        this.catalog = new HiveCatalog();

        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);

        // Extract metastore URI from Hadoop config if present
        String metastoreUri = hadoopConf.get("hive.metastore.uris");
        if (metastoreUri != null) {
            properties.put(CatalogProperties.URI, metastoreUri);
        }

        catalog.setConf(hadoopConf);
        catalog.initialize(catalogName, properties);

        LOG.info(
                "Initialized Hive Metastore catalog '{}' from Hadoop configuration with warehouse {}",
                catalogName,
                warehouse);
    }

    @Override
    protected Catalog getCatalog() {
        return catalog;
    }

    @Override
    public void close() {
        try {
            catalog.close();
        } catch (Exception e) {
            LOG.warn("Error closing Hive catalog '{}': {}", catalogName, e.getMessage());
        }
    }
}
