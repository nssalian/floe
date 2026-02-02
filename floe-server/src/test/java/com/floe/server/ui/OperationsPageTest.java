package com.floe.server.ui;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class OperationsPageTest {

    @Test
    void testOperationsShowsDuration() throws IOException {
        String template = readTemplate("operations.html");
        assertTrue(template.contains("Duration"), "duration column missing");
    }

    @Test
    void testOperationsShowsEngineType() throws IOException {
        String template = readTemplate("operations.html");
        assertTrue(template.contains("Engine"), "engine column missing");
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
