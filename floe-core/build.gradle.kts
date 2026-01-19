plugins {
    `java-library`
}

dependencies {
    // Iceberg core
    api(libs.iceberg.core)
    api(libs.iceberg.api)

    // Iceberg catalog implementations
    implementation(libs.iceberg.bundled.guava)
    implementation(libs.iceberg.aws)
    implementation(libs.iceberg.hive.metastore)
    implementation(libs.iceberg.nessie) {
        // Exclude OpenAPI dependencies that conflict with Quarkus
        exclude(group = "org.eclipse.microprofile.openapi")
        exclude(group = "io.smallrye", module = "smallrye-open-api-core")
    }

    // AWS SDK
    implementation(libs.aws.s3)
    implementation(libs.aws.sts)
    implementation(libs.aws.kms)
    implementation(libs.aws.url.connection.client)
    implementation(libs.aws.apache.client)

    // Hive Metastore client
    implementation(libs.hive.metastore) {
        exclude(group = "org.slf4j")
        exclude(group = "log4j")
        exclude(group = "commons-logging")
        exclude(group = "org.apache.logging.log4j")
        exclude(group = "org.pentaho")
    }

    // For Spark job submission
    implementation(libs.spark.launcher)

    // Configuration and YAML parsing
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.jackson.datatype.jsr310)
    implementation(libs.jackson.datatype.jdk8)

    // Logging
    implementation(libs.slf4j.api)

    // HTTP client for REST catalog
    implementation(libs.httpclient5)

    // Hadoop (for FileIO and Hive Metastore)
    implementation(libs.hadoop.common) {
        exclude(group = "org.slf4j")
        exclude(group = "log4j")
        exclude(group = "commons-logging")
    }
    // Required by Hive Metastore for HiveConf
    implementation(libs.hadoop.mapreduce.client.core) {
        exclude(group = "org.slf4j")
        exclude(group = "log4j")
        exclude(group = "commons-logging")
    }

    // For Postgres
    implementation(libs.postgresql)

    // Testing
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.localstack)
    testImplementation(libs.logback.classic)
}
