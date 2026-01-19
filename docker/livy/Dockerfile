# Dockerfile for Floe Livy + Spark
#
# Build (from repo root):
#   ./gradlew :floe-livy-job:shadowJar
#   docker build -t floe-livy:v0.1.0 -f docker/livy/Dockerfile .

FROM eclipse-temurin:19-jre-focal

ARG LIVY_VERSION=0.8.0-incubating
ARG SPARK_VERSION=3.5.5
ARG ICEBERG_VERSION=1.10.0

ENV LIVY_HOME=/opt/livy
ENV SPARK_HOME=/opt/spark
ENV LIVY_CONF_DIR="${LIVY_HOME}/conf"

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN curl -fSL --retry 3 --retry-delay 10 --max-time 600 \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    -o "/tmp/spark.tgz" \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" "${SPARK_HOME}" \
    && rm /tmp/spark.tgz

# Download and install Livy
RUN curl --progress-bar -L --retry 3 \
    "https://dlcdn.apache.org/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}_2.12-bin.zip" \
    -o "/tmp/livy.zip" \
    && unzip -qq "/tmp/livy.zip" -d /opt \
    && mv "/opt/apache-livy-${LIVY_VERSION}_2.12-bin" "${LIVY_HOME}" \
    && rm "/tmp/livy.zip" \
    && mkdir -p "${LIVY_HOME}/logs"

# Download Iceberg Spark runtime
RUN curl --progress-bar -L --retry 3 \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    -o "${SPARK_HOME}/jars/iceberg-spark-runtime.jar"

# Download Iceberg AWS bundle for S3 support (includes compatible AWS SDK v2)
RUN curl --progress-bar -L --retry 3 \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    -o "${SPARK_HOME}/jars/iceberg-aws-bundle.jar"

# Download hadoop-aws for S3AFileSystem (needed for orphan cleanup)
RUN curl --progress-bar -L --retry 3 \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    -o "${SPARK_HOME}/jars/hadoop-aws.jar"

# Download AWS SDK v1 bundle (required by hadoop-aws)
RUN curl --progress-bar -L --retry 3 \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
    -o "${SPARK_HOME}/jars/aws-java-sdk-bundle.jar"

# Download Nessie Spark extensions for Nessie catalog support
RUN curl --progress-bar -L --retry 3 \
    "https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.76.0/nessie-spark-extensions-3.5_2.12-0.76.0.jar" \
    -o "${SPARK_HOME}/jars/nessie-spark-extensions.jar"

# Copy entrypoint script
COPY docker/livy/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Copy Livy config
COPY docker/livy/livy.conf ${LIVY_CONF_DIR}/livy.conf
COPY docker/livy/livy-env.sh ${LIVY_CONF_DIR}/livy-env.sh

# Bundle the Floe maintenance job JAR
# This is the Spark job that performs Iceberg maintenance operations
# Build with: ./gradlew :floe-spark-job:shadowJar
RUN mkdir -p /opt/floe
COPY floe-spark-job/build/libs/floe-maintenance-job-*.jar /opt/floe/floe-maintenance-job.jar

EXPOSE 8998

ENTRYPOINT ["/entrypoint.sh"]
