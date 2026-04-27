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

import java.util.Objects;

/**
 * Identifies an Iceberg table by catalog, namespace (database), and table name.
 *
 * @param catalog catalog name
 * @param namespace namespace (database) name
 * @param table table name
 */
public record TableIdentifier(String catalog, String namespace, String table) {
    public TableIdentifier {
        Objects.requireNonNull(catalog, "catalog cannot be null");
        Objects.requireNonNull(namespace, "namespace cannot be null");
        Objects.requireNonNull(table, "table cannot be null");
    }

    /** Create a TableIdentifier from components */
    public static TableIdentifier of(String catalog, String namespace, String table) {
        return new TableIdentifier(catalog, namespace, table);
    }

    /** Parse a fully qualified table name like "catalog.database.table" */
    public static TableIdentifier parse(String qualifiedName) {
        if (qualifiedName == null || qualifiedName.isBlank()) {
            throw new IllegalArgumentException("qualifiedName cannot be null or blank");
        }
        String[] parts = qualifiedName.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException(
                    "Expected format: catalog.namespace.table, got: " + qualifiedName);
        }
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].isBlank()) {
                throw new IllegalArgumentException(
                        "Empty segment at position " + i + " in: " + qualifiedName);
            }
        }
        return new TableIdentifier(parts[0], parts[1], parts[2]);
    }

    /** Returns the fully qualified name: catalog.namespace.table */
    public String toQualifiedName() {
        return String.join(".", catalog, namespace, table);
    }

    /** Returns namespace.table format for Iceberg API calls */
    public String toNamespaceTable() {
        return String.join(".", namespace, table);
    }

    @Override
    public String toString() {
        return toQualifiedName();
    }

    public String getCatalog() {
        return catalog;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getTableName() {
        return table;
    }
}
