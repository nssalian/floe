# Dockerfile for Floe Server
# Build:
#   ./gradlew :floe-server:quarkusBuild -Dquarkus.package.type=fast-jar
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# Create non-root user for security
RUN addgroup -S floe && adduser -S floe -G floe

# Copy the pre-built Quarkus application
COPY floe-server/build/quarkus-app/lib/ /app/lib/
COPY floe-server/build/quarkus-app/*.jar /app/
COPY floe-server/build/quarkus-app/app/ /app/app/
COPY floe-server/build/quarkus-app/quarkus/ /app/quarkus/

# Set ownership
RUN chown -R floe:floe /app

USER floe

# Expose the application port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/q/health/ready || exit 1

# JVM options optimized for containers
ENV JAVA_OPTS="-Xms128m -Xmx512m \
    -XX:+UseG1GC \
    -XX:+UseContainerSupport \
    -XX:MaxRAMPercentage=75.0 \
    -Djava.security.egd=file:/dev/./urandom"

# Start the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/quarkus-run.jar"]
