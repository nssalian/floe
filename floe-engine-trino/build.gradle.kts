plugins {
    `java-library`
}

description = "Floe Trino Execution Engine - Execute maintenance via Trino SQL"

dependencies {
    api(project(":floe-core"))

    implementation(libs.trino.jdbc)
    implementation(libs.hikaricp)
    implementation(libs.slf4j.api)
}

tasks.test {
    useJUnitPlatform()
}
