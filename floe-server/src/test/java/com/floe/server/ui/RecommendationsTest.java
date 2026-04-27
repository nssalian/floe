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

package com.floe.server.ui;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class RecommendationsTest {

    @Test
    void testRecommendationsShowsPendingOps() throws IOException {
        String template = readTemplate("table-detail.html");
        assertTrue(template.contains("Recommended Maintenance"), "recommendations header missing");
    }

    @Test
    void testRecommendationsShowsReason() throws IOException {
        String template = readTemplate("table-detail.html");
        assertTrue(template.contains("{rec.reason}"), "recommendation reason binding missing");
    }

    @Test
    void testRecommendationsTriggerButton() throws IOException {
        String template = readTemplate("table-detail.html");
        assertTrue(template.contains("Trigger"), "trigger button missing");
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
