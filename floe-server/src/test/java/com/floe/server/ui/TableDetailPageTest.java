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
