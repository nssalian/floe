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

plugins {
    java
    alias(libs.plugins.quarkus)
}

dependencies {
    implementation(project(":floe-core"))
    implementation(project(":floe-spark-job"))
    implementation(project(":floe-engine-spark"))
    implementation(project(":floe-engine-trino"))

    // REST API - resteasy-core MUST be before quarkus deps (jandex fix)
    implementation(libs.resteasy.core)
    implementation(libs.quarkus.resteasy.reactive)
    implementation(libs.quarkus.resteasy.reactive.jackson)
    implementation(libs.quarkus.arc)
    implementation(libs.quarkus.scheduler)
    implementation(libs.quarkus.smallrye.openapi)
    implementation(libs.quarkus.micrometer.registry.prometheus)
    implementation(libs.quarkus.smallrye.health)
    implementation(libs.quarkus.hibernate.validator)

    // Authentication & Authorization
    implementation(libs.quarkus.security)
    implementation(libs.quarkus.oidc)
    implementation(libs.quarkus.oidc.token.propagation)
    implementation(libs.smallrye.jwt)

    // Netty DNS resolver for macOS (suppresses warning)
    implementation(variantOf(libs.netty.resolver.dns.macos) { classifier("osx-aarch_64") })

    // Cron expression validation
    implementation(libs.quartz)

    // Database
    implementation(libs.quarkus.agroal)
    implementation(libs.quarkus.jdbc.postgresql)
    implementation(libs.quarkus.flyway)

    // YAML configuration support
    implementation(libs.quarkus.config.yaml)

    // Server-side rendering with Qute
    implementation(libs.quarkus.resteasy.reactive.qute)

    // Webjars for frontend assets
    implementation(libs.htmx)
    implementation(libs.webjars.locator.core)

    testImplementation(libs.quarkus.junit5)
    testImplementation(libs.rest.assured)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit.jupiter)

    // Testcontainers for integration tests
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.hikaricp)

    // Awaitility for async testing
    testImplementation(libs.awaitility)
}

tasks.withType<JavaExec> {
    jvmArgs = listOf(
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens", "java.base/java.nio=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang=ALL-UNNAMED",
        "--add-opens", "java.base/java.util=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang.invoke=ALL-UNNAMED"
    )
}

tasks.named("compileJava") {
    dependsOn("compileQuarkusGeneratedSourcesJava")
}

// E2E Tests
tasks.register<Test>("e2eTest") {
    description = "Runs end-to-end tests with Testcontainers"
    group = "verification"

    useJUnitPlatform {
        includeTags("e2e")
    }

    shouldRunAfter(tasks.named("test"))
    shouldRunAfter(tasks.named("integrationTest"))

    // added more time for e2e tests
    systemProperty("junit.jupiter.execution.timeout.default", "5m")

    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
    }
}
