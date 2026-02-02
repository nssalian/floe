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
