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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TableIdentifierTest {

    @Test
    void shouldCreateTableIdentifier() {
        TableIdentifier id = new TableIdentifier("catalog", "database", "table");

        assertThat(id.catalog()).isEqualTo("catalog");
        assertThat(id.namespace()).isEqualTo("database");
        assertThat(id.table()).isEqualTo("table");
    }

    @Test
    void shouldParseQualifiedName() {
        TableIdentifier id = TableIdentifier.parse("my_catalog.my_db.my_table");

        assertThat(id.catalog()).isEqualTo("my_catalog");
        assertThat(id.namespace()).isEqualTo("my_db");
        assertThat(id.table()).isEqualTo("my_table");
    }

    @Test
    void shouldFormatToQualifiedName() {
        TableIdentifier id = new TableIdentifier("catalog", "database", "table");

        assertThat(id.toQualifiedName()).isEqualTo("catalog.database.table");
        assertThat(id.toString()).isEqualTo("catalog.database.table");
    }

    @Test
    void shouldFormatToNamespaceTable() {
        TableIdentifier id = new TableIdentifier("catalog", "database", "table");

        assertThat(id.toNamespaceTable()).isEqualTo("database.table");
    }

    @Test
    void shouldRejectInvalidQualifiedName() {
        assertThatThrownBy(() -> TableIdentifier.parse("only.two"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected format: catalog.namespace.table");
    }

    @Test
    void shouldRejectNullValues() {
        assertThatThrownBy(() -> new TableIdentifier(null, "db", "table"))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> new TableIdentifier("cat", null, "table"))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> new TableIdentifier("cat", "db", null))
                .isInstanceOf(NullPointerException.class);
    }
}
