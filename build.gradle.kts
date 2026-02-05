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

import net.ltgt.gradle.errorprone.errorprone

plugins {
    java
    idea
    alias(libs.plugins.spotless) apply false
    alias(libs.plugins.errorprone) apply false
}

allprojects {
    group = "com.floe"
    version = "0.1.0"

    repositories {
        mavenCentral()
    }
}

val googleJavaFormatVersion = libs.versions.google.java.format.get()

subprojects {
    apply(plugin = "java")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "net.ltgt.errorprone")

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.compilerArgs.add("-parameters")
        // ErrorProne configuration
        options.errorprone {
            disableAllWarnings.set(true)
            disableWarningsInGeneratedCode.set(true)
            // Promote critical checks to errors
            error(
                "DefaultCharset",
                "FallThrough",
                "JavaTimeDefaultTimeZone",
                "MissingCasesInEnumSwitch",
                "MissingOverride",
                "ModifiedButNotUsed",
                "OrphanedFormatString",
                "StringCaseLocaleUsage",
            )
        }
    }

    dependencies {
        "errorprone"(rootProject.libs.errorprone.core)
    }

    tasks.named<Test>("test") {
        useJUnitPlatform {
            excludeTags("integration", "e2e")
        }
        // Run unit tests in parallel for speed
        maxParallelForks = (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(1)
        // Suppress test output noise
        testLogging {
            showStandardStreams = false
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.SHORT
        }
        // Suppress JVM class sharing warnings
        jvmArgs("-Xshare:off")
    }

    tasks.register<Test>("integrationTest") {
        description = "Runs integration tests (requires Docker services)"
        group = "verification"
        useJUnitPlatform {
            includeTags("integration")
        }
        // Integration tests use Docker - limit parallelism to avoid resource contention
        maxParallelForks = 2
        // Suppress test output noise
        testLogging {
            showStandardStreams = false
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.SHORT
        }
        // Suppress JVM class sharing warnings
        jvmArgs("-Xshare:off")
        shouldRunAfter(tasks.named("test"))

    }

    configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        java {
            target("src/main/java/**/*.java", "src/test/java/**/*.java")
            googleJavaFormat(googleJavaFormatVersion).aosp()
            removeUnusedImports()
            trimTrailingWhitespace()
            endWithNewline()
        }
    }

    if (!providers.environmentVariable("CI").isPresent) {
        tasks.named("build") {
            dependsOn("spotlessApply")
        }
    }



    dependencies {
        testImplementation(platform(rootProject.libs.junit.bom))
        testImplementation(rootProject.libs.junit.jupiter)
        testImplementation(rootProject.libs.assertj.core)
        testImplementation(rootProject.libs.mockito.core)
        testImplementation(rootProject.libs.mockito.junit.jupiter)
    }
}

tasks.register("codeSmellCheck") {
    group = "verification"
    description = "Runs formatting check across all modules"

    dependsOn(subprojects.map { "${it.path}:spotlessCheck" })
}

tasks.register("ciTest") {
    group = "verification"
    description = "Runs unit + integration tests for CI"
    dependsOn(subprojects.flatMap {
        listOf("${it.path}:test", "${it.path}:integrationTest")
    })
}
