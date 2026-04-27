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

package com.floe.core.policy;

import static org.junit.jupiter.api.Assertions.*;

import com.floe.core.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TablePatternTest {

    @Test
    void parseFullyQualifiedPattern() {
        TablePattern pattern = TablePattern.parse("catalog.namespace.table");

        assertEquals("catalog", pattern.catalogPattern());
        assertEquals("namespace", pattern.namespacePattern());
        assertEquals("table", pattern.tablePattern());
    }

    @Test
    void parseNamespaceTablePattern() {
        TablePattern pattern = TablePattern.parse("namespace.table");

        assertNull(pattern.catalogPattern());
        assertEquals("namespace", pattern.namespacePattern());
        assertEquals("table", pattern.tablePattern());
    }

    @Test
    void parseTableOnlyPattern() {
        TablePattern pattern = TablePattern.parse("table");

        assertNull(pattern.catalogPattern());
        assertNull(pattern.namespacePattern());
        assertEquals("table", pattern.tablePattern());
    }

    @Test
    void parseWildcardPatterns() {
        TablePattern pattern = TablePattern.parse("*.namespace.*");

        assertNull(pattern.catalogPattern());
        assertEquals("namespace", pattern.namespacePattern());
        assertNull(pattern.tablePattern());
    }

    @Test
    void matchAllPattern() {
        TablePattern pattern = TablePattern.matchAll();
        TableIdentifier tableId = TableIdentifier.of("prod", "any_namespace", "any_table");

        assertTrue(pattern.matches("any_catalog", tableId));
    }

    @Test
    void exactMatchPattern() {
        TablePattern pattern = TablePattern.exact("prod", "analytics", "events");

        TableIdentifier matching = TableIdentifier.of("prod", "analytics", "events");
        TableIdentifier nonMatching = TableIdentifier.of("prod", "analytics", "other");

        assertTrue(pattern.matches("prod", matching));
        assertFalse(pattern.matches("prod", nonMatching));
        assertFalse(pattern.matches("dev", matching));
    }

    @ParameterizedTest
    @CsvSource({
        "prod.*.events, prod, analytics, events, true",
        "prod.*.events, prod, streaming, events, true",
        "prod.*.events, dev, analytics, events, false",
        "prod.analytics.*, prod, analytics, events, true",
        "prod.analytics.*, prod, analytics, users, true",
        "prod.analytics.*, prod, other, events, false",
        "*.analytics.*, prod, analytics, events, true",
        "*.analytics.*, dev, analytics, users, true",
        "*.analytics.*, prod, other, events, false",
    })
    void wildcardMatching(
            String patternStr,
            String catalog,
            String namespace,
            String table,
            boolean shouldMatch) {
        TablePattern pattern = TablePattern.parse(patternStr);
        TableIdentifier tableId = TableIdentifier.of(catalog, namespace, table);

        assertEquals(
                shouldMatch,
                pattern.matches(catalog, tableId),
                () ->
                        String.format(
                                "Pattern '%s' should%s match '%s.%s.%s'",
                                patternStr, shouldMatch ? "" : " not", catalog, namespace, table));
    }

    @ParameterizedTest
    @CsvSource({
        "prod.prod_*.*, prod, prod_db1, events, true",
        "prod.prod_*.*, prod, prod_db2, users, true",
        "prod.prod_*.*, prod, dev_db, events, false",
        "*.*.events_*, prod, analytics, events_v1, true",
        "*.*.events_*, prod, analytics, events_v2, true",
        "*.*.events_*, prod, analytics, users, false",
    })
    void globPatternMatching(
            String patternStr,
            String catalog,
            String namespace,
            String table,
            boolean shouldMatch) {
        TablePattern pattern = TablePattern.parse(patternStr);
        TableIdentifier tableId = TableIdentifier.of(catalog, namespace, table);

        assertEquals(shouldMatch, pattern.matches(catalog, tableId));
    }

    @Test
    void specificity() {
        TablePattern allWild = TablePattern.parse("*.*.*");
        TablePattern catalogOnly = TablePattern.parse("prod.*.*");
        TablePattern catalogNamespace = TablePattern.parse("prod.analytics.*");
        TablePattern fullyQualified = TablePattern.parse("prod.analytics.events");

        assertTrue(fullyQualified.specificity() > catalogNamespace.specificity());
        assertTrue(catalogNamespace.specificity() > catalogOnly.specificity());
        assertTrue(catalogOnly.specificity() > allWild.specificity());
    }

    @Test
    void toStringFormat() {
        TablePattern pattern = TablePattern.parse("prod.*.events");
        assertEquals("prod.*.events", pattern.toString());
    }

    @Test
    void namespaceHelper() {
        TablePattern pattern = TablePattern.namespace("prod", "analytics");
        TableIdentifier events = TableIdentifier.of("dev", "analytics", "events");
        TableIdentifier users = TableIdentifier.of("prod", "analytics", "users");
        TableIdentifier other = TableIdentifier.of("dev", "analytics", "events");

        assertTrue(pattern.matches("prod", events));
        assertTrue(pattern.matches("prod", users));
        assertFalse(pattern.matches("dev", other));
    }

    @Test
    void catalogHelper() {
        TablePattern pattern = TablePattern.catalog("prod");
        TableIdentifier any1 = TableIdentifier.of("prod", "ns1", "t1");
        TableIdentifier any2 = TableIdentifier.of("dev", "n2", "t2");

        assertTrue(pattern.matches("prod", any1));
        assertTrue(pattern.matches("prod", any2));
        assertFalse(pattern.matches("dev", any1));
    }
}
