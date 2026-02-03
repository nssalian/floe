package com.floe.core.policy;

import com.floe.core.catalog.TableIdentifier;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Pattern for matching Iceberg tables by catalog, namespace (database), and table name.
 *
 * <p>Supports: - Exact matches: "catalog.database.table" - Wildcards: "catalog.*.table" or
 * "catalog.database.*" - Glob patterns: "catalog.prod_*.*" - Regex: "/catalog\.prod_\d+\..*\/"
 *
 * @param catalogPattern catalog pattern (null matches all)
 * @param namespacePattern namespace pattern (null matches all)
 * @param tablePattern table pattern (null matches all)
 */
public record TablePattern(String catalogPattern, String namespacePattern, String tablePattern) {
    private static final String WILDCARD = "*";
    private static final int MAX_PATTERN_LENGTH = 256;
    private static final int MAX_REGEX_LENGTH = 128;

    private static final ConcurrentHashMap<String, Pattern> PATTERN_CACHE =
            new ConcurrentHashMap<>();

    /**
     * Parse a dot-separated pattern string. Formats: - "table" -> matches table in any
     * catalog/namespace - "namespace.table" -> matches in any catalog - "catalog.namespace.table"
     * -> fully qualified
     *
     * <p>Use "*" for wildcards at any level.
     *
     * @throws IllegalArgumentException if pattern is too long or contains invalid regex
     */
    public static TablePattern parse(String pattern) {
        if (pattern == null || pattern.isBlank()) {
            return matchAll();
        }

        if (pattern.length() > MAX_PATTERN_LENGTH) {
            throw new IllegalArgumentException(
                    "Pattern too long (max "
                            + MAX_PATTERN_LENGTH
                            + " characters): "
                            + pattern.length());
        }

        if (pattern.contains("/")) {
            validateRegexPatterns(pattern);
        }

        String[] parts = pattern.split("\\.", 3);

        return switch (parts.length) {
            case 1 -> new TablePattern(null, null, parts[0]);
            case 2 -> new TablePattern(null, parts[0], parts[1]);
            case 3 ->
                    new TablePattern(
                            parts[0].equals(WILDCARD) ? null : parts[0],
                            parts[1].equals(WILDCARD) ? null : parts[1],
                            parts[2].equals(WILDCARD) ? null : parts[2]);
            default -> throw new IllegalArgumentException("Invalid pattern: " + pattern);
        };
    }

    /** Match all tables. */
    public static TablePattern matchAll() {
        return new TablePattern(null, null, null);
    }

    /** Match a specific table. */
    public static TablePattern exact(String catalog, String namespace, String table) {
        return new TablePattern(catalog, namespace, table);
    }

    /** Match all tables in a namespace. */
    public static TablePattern namespace(String catalog, String namespace) {
        return new TablePattern(catalog, namespace, null);
    }

    /** Match all tables in a catalog. */
    public static TablePattern catalog(String catalog) {
        return new TablePattern(catalog, null, null);
    }

    /** Check if this pattern matches the given fully-qualified table name. */
    public boolean matches(String catalog, String namespace, TableIdentifier table) {
        return (matchesCatalog(catalog)
                && matchesNamespace(namespace)
                && matchesTable(table.getTableName()));
    }

    /** Check if this pattern matches the given table identifier. */
    public boolean matches(String catalog, TableIdentifier tableId) {
        return (matchesCatalog(catalog)
                && matchesNamespace(tableId.getNamespace())
                && matchesTable(tableId.getTableName()));
    }

    private boolean matchesPattern(String pattern, String value) {
        if (pattern == null || pattern.equals(WILDCARD)) {
            return true;
        }
        if (value == null) {
            return false;
        }

        // Check for regex pattern (enclosed in /.../)
        if (pattern.startsWith("/") && pattern.endsWith("/")) {
            String regex = pattern.substring(1, pattern.length() - 1);
            Pattern compiledPattern = PATTERN_CACHE.computeIfAbsent(regex, Pattern::compile);
            return compiledPattern.matcher(value).matches();
        }

        // Convert glob pattern to regex
        if (pattern.contains("*") || pattern.contains("?")) {
            String regex = globToRegex(pattern);
            Pattern compiledPattern = PATTERN_CACHE.computeIfAbsent(regex, Pattern::compile);
            return compiledPattern.matcher(value).matches();
        }

        // Exact match
        return pattern.equals(value);
    }

    /**
     * Validate regex patterns in a pattern string to prevent ReDoS attacks. Called during parse()
     * to fail fast on invalid patterns.
     */
    private static void validateRegexPatterns(String pattern) {
        String[] parts = pattern.split("\\.", 3);
        for (String part : parts) {
            if (part.startsWith("/") && part.endsWith("/") && part.length() > 2) {
                String regex = part.substring(1, part.length() - 1);

                if (regex.length() > MAX_REGEX_LENGTH) {
                    throw new IllegalArgumentException(
                            "Regex pattern too long (max " + MAX_REGEX_LENGTH + " characters)");
                }

                // Check for potentially dangerous regex patterns (nested quantifiers)
                if (containsDangerousPattern(regex)) {
                    throw new IllegalArgumentException(
                            "Regex pattern contains potentially dangerous constructs (nested quantifiers)");
                }

                // Try to compile the regex to catch syntax errors
                try {
                    Pattern.compile(regex);
                } catch (PatternSyntaxException e) {
                    throw new IllegalArgumentException("Invalid regex pattern: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Check for regex patterns that could cause catastrophic backtracking (ReDoS). Detects patterns
     * like (a+)+ or (a|a)+ that have exponential complexity.
     */
    private static boolean containsDangerousPattern(String regex) {
        // Detect nested quantifiers: patterns like (x+)+, (x*)+, (x+)*, etc.
        return (regex.matches(".*\\([^)]*[+*][^)]*\\)[+*].*")
                || regex.matches(".*[+*]\\([^)]*[+*].*")
                ||
                // Detect overlapping alternations with quantifiers: (a|a)+
                regex.matches(".*\\(([^|)]+)\\|\\1\\)[+*].*"));
    }

    private String globToRegex(String glob) {
        StringBuilder regex = new StringBuilder("^");
        for (char c : glob.toCharArray()) {
            switch (c) {
                case '*' -> regex.append(".*");
                case '?' -> regex.append(".");
                case '.' -> regex.append("\\.");
                case '\\' -> regex.append("\\\\");
                case '[', ']', '(', ')', '{', '}', '^', '$', '|', '+' ->
                        regex.append("\\").append(c);
                default -> regex.append(c);
            }
        }
        regex.append("$");
        return regex.toString();
    }

    private boolean matchesCatalog(String catalog) {
        return (catalogPattern == null || matchesPattern(catalogPattern, catalog));
    }

    private boolean matchesNamespace(String namespace) {
        return (namespacePattern == null || matchesPattern(namespacePattern, namespace));
    }

    private boolean matchesTable(String table) {
        return tablePattern == null || matchesPattern(tablePattern, table);
    }

    /**
     * Get specificity score for ordering patterns (more specific = higher score). Used to determine
     * which policy takes precedence when multiple match.
     */
    public int specificity() {
        int score = 0;
        if (catalogPattern != null && !catalogPattern.contains("*")) score += 100;
        else if (catalogPattern != null) score += 50;

        if (namespacePattern != null && !namespacePattern.contains("*")) score += 10;
        else if (namespacePattern != null) score += 5;

        if (tablePattern != null && !tablePattern.contains("*")) score += 1;
        else if (tablePattern != null) score += 0;

        return score;
    }

    @Override
    public String toString() {
        return String.format(
                "%s.%s.%s",
                catalogPattern != null ? catalogPattern : "*",
                namespacePattern != null ? namespacePattern : "*",
                tablePattern != null ? tablePattern : "*");
    }
}
