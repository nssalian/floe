plugins {
    `java-library`
    alias(libs.plugins.shadow)
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

description = "Floe Spark Job - Maintenance job submitted via Livy"

dependencies {
    compileOnly(libs.iceberg.spark.runtime)
    compileOnly(libs.spark.sql)
    implementation(libs.jackson.databind)
    implementation(libs.slf4j.api)

    testRuntimeOnly(libs.slf4j.simple)
    testImplementation(libs.spark.sql)
    testImplementation(libs.iceberg.spark.runtime)
}

tasks.shadowJar {
    archiveBaseName.set("floe-maintenance-job")
    archiveClassifier.set("")
    archiveVersion.set(project.version.toString())

    dependencies {
        exclude(dependency("org.apache.spark:.*"))
        exclude(dependency("org.apache.iceberg:.*"))
    }

    manifest {
        attributes["Main-Class"] = "com.floe.spark.job.MaintenanceJob"
    }
}

tasks.build {
    dependsOn(tasks.shadowJar)
}
