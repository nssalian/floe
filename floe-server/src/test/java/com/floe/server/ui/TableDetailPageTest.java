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

class TableDetailPageTest {

    @Test
    void testTableDetailRendersHealthTrend() throws IOException {
        String template = readTemplate("table-detail.html");
        assertTrue(template.contains("health-trend"), "health trend section missing");
    }

    @Test
    void testTableDetailTrendDataEndpoint() throws IOException {
        String template = readTemplate("health-trend.html");
        assertTrue(template.contains("/api/v1/tables/"), "health history endpoint missing");
    }

    @Test
    void testTableDetailChartLabels() throws IOException {
        String template = readTemplate("health-trend.html");
        assertTrue(template.contains("Health Trend"), "trend title missing");
        assertTrue(template.contains("Score"), "score label missing");
    }

    @Test
    void testTableDetailEmptyHistoryShowsMessage() throws IOException {
        String template = readTemplate("health-trend.html");
        assertTrue(template.contains("No health history"), "empty history message missing");
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
