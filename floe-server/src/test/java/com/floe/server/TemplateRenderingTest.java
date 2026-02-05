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

package com.floe.server;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class TemplateRenderingTest {

    @Test
    void testHealthTrendPartialCompiles() throws IOException {
        String template = readTemplate("health-trend.html");
        assertFalse(template.isBlank(), "health-trend template is empty");
        assertTrue(template.contains("health-trend"), "health-trend container missing");
    }

    @Test
    void testTableDetailCompiles() throws IOException {
        String template = readTemplate("table-detail.html");
        assertFalse(template.isBlank(), "table-detail template is empty");
        assertTrue(template.contains("Recommended Maintenance"), "recommendations section missing");
    }

    @Test
    void testOperationsCompiles() throws IOException {
        String template = readTemplate("operations.html");
        assertFalse(template.isBlank(), "operations template is empty");
        assertTrue(template.contains("Operations"), "operations header missing");
    }

    private static String readTemplate(String name) throws IOException {
        Path path = resolveTemplatePath(name);
        return Files.readString(path);
    }

    private static Path resolveTemplatePath(String name) {
        Path path = Path.of("src/main/resources/templates", name);
        if (Files.exists(path)) {
            return path;
        }
        return Path.of("floe-server/src/main/resources/templates", name);
    }
}
