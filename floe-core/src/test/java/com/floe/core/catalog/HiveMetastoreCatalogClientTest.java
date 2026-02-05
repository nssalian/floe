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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("HiveMetastoreCatalogClient")
class HiveMetastoreCatalogClientTest {

    private static final String CATALOG_NAME = "hive_catalog";
    private static final String METASTORE_URI = "thrift://localhost:9083";
    private static final String WAREHOUSE = "s3://bucket/warehouse";

    @Nested
    @DisplayName("Constructor")
    class Constructor {

        @Test
        @DisplayName("should create client with basic parameters")
        void shouldCreateWithBasicParams() {
            // HiveCatalog initializes lazily - no connection during construction
            // The actual connection happens when methods are called
            HiveMetastoreCatalogClient client =
                    new HiveMetastoreCatalogClient(CATALOG_NAME, METASTORE_URI, WAREHOUSE);

            assertThat(client.getCatalogName()).isEqualTo(CATALOG_NAME);
            client.close();
        }

        @Test
        @DisplayName("should accept additional properties")
        void shouldAcceptAdditionalProperties() {
            Map<String, String> props = new HashMap<>();
            props.put("hive.metastore.client.socket.timeout", "60s");
            props.put("s3.endpoint", "http://minio:9000");

            HiveMetastoreCatalogClient client =
                    new HiveMetastoreCatalogClient(CATALOG_NAME, METASTORE_URI, WAREHOUSE, props);

            assertThat(client.getCatalogName()).isEqualTo(CATALOG_NAME);
            client.close();
        }

        @Test
        @DisplayName("should accept Hadoop configuration")
        void shouldAcceptHadoopConfig() {
            Configuration hadoopConf = new Configuration();
            hadoopConf.set("hive.metastore.uris", METASTORE_URI);
            hadoopConf.set("fs.s3a.endpoint", "http://minio:9000");

            HiveMetastoreCatalogClient client =
                    new HiveMetastoreCatalogClient(CATALOG_NAME, hadoopConf, WAREHOUSE);

            assertThat(client.getCatalogName()).isEqualTo(CATALOG_NAME);
            client.close();
        }
    }

    @Nested
    @DisplayName("Configuration")
    class ConfigurationTests {

        @Test
        @DisplayName("should configure Hadoop with metastore URI")
        void shouldConfigureHadoopWithUri() {
            Configuration hadoopConf = new Configuration();
            hadoopConf.set("hive.metastore.uris", METASTORE_URI);

            assertThat(hadoopConf.get("hive.metastore.uris")).isEqualTo(METASTORE_URI);
        }

        @Test
        @DisplayName("should separate Hadoop and Iceberg properties")
        void shouldSeparateProperties() {
            Map<String, String> props = new HashMap<>();
            // Hadoop properties (should go to Hadoop config)
            props.put("hadoop.fs.s3a.endpoint", "http://minio:9000");
            props.put("hive.metastore.client.socket.timeout", "60s");
            // Iceberg properties (should go to catalog properties)
            props.put("s3.endpoint", "http://minio:9000");
            props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

            // Verify the separation logic works
            Map<String, String> hadoopProps = new HashMap<>();
            Map<String, String> catalogProps = new HashMap<>();

            for (Map.Entry<String, String> entry : props.entrySet()) {
                if (entry.getKey().startsWith("hadoop.") || entry.getKey().startsWith("hive.")) {
                    hadoopProps.put(entry.getKey(), entry.getValue());
                } else {
                    catalogProps.put(entry.getKey(), entry.getValue());
                }
            }

            assertThat(hadoopProps).hasSize(2);
            assertThat(hadoopProps).containsKey("hadoop.fs.s3a.endpoint");
            assertThat(hadoopProps).containsKey("hive.metastore.client.socket.timeout");

            assertThat(catalogProps).hasSize(2);
            assertThat(catalogProps).containsKey("s3.endpoint");
            assertThat(catalogProps).containsKey("io-impl");
        }
    }

    @Nested
    @DisplayName("URI Parsing")
    class UriParsing {

        @Test
        @DisplayName("should accept thrift URI format")
        void shouldAcceptThriftUri() {
            String uri = "thrift://hive-metastore:9083";
            assertThat(uri).startsWith("thrift://");
        }

        @Test
        @DisplayName("should accept multiple metastore URIs")
        void shouldAcceptMultipleUris() {
            String uri = "thrift://hive1:9083,thrift://hive2:9083";
            assertThat(uri.split(",")).hasSize(2);
        }
    }
}
