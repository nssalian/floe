plugins {
    `java-library`
}

description = "Floe Spark Execution Engine - Submit maintenance jobs via Apache Livy"

dependencies {
    // Floe core
    api(project(":floe-core"))

    implementation(libs.jackson.databind)
    implementation(libs.jackson.datatype.jdk8)
    implementation(libs.jackson.datatype.jsr310)

    implementation(libs.slf4j.api)
}

tasks.test {
    useJUnitPlatform()
}
