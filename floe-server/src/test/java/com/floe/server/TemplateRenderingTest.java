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
