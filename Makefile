.PHONY: build clean start test integration-test e2e fmt \
	example-rest example-nessie example-polaris example-hms example-lakekeeper example-gravitino \
	example-rest-spark example-rest-trino example-rest-spark-local example-rest-trino-local \
	example-nessie-spark example-nessie-trino example-nessie-spark-local example-nessie-trino-local \
	example-polaris-spark example-polaris-trino example-polaris-spark-local example-polaris-trino-local \
	example-hms-spark example-hms-trino example-hms-spark-local example-hms-trino-local \
	example-lakekeeper-spark example-lakekeeper-trino example-lakekeeper-spark-local example-lakekeeper-trino-local \
	example-gravitino-spark example-gravitino-trino example-gravitino-spark-local example-gravitino-trino-local

# Default images
FLOE_IMAGE ?= ghcr.io/nssalian/floe:latest
FLOE_LIVY_IMAGE ?= ghcr.io/nssalian/floe-livy:latest
SKIP_DEMO ?= 0

build:
	@echo "Building Floe..."
	@./gradlew clean build -x test --parallel --max-workers=10
	@docker build -t floe:local -f docker/floe-server/Dockerfile .
	@./gradlew :floe-spark-job:shadowJar -x test
	@docker build -t floe-livy:local -f docker/livy/Dockerfile .
	@echo "Build complete"

test:
	@echo "Running unit tests..."
	@./gradlew test --parallel --max-workers=10
	@echo "Tests complete"

integration-test:
	@echo "Running integration tests..."
	@echo "NOTE: Requires Docker running (uses Testcontainers)"
	@./gradlew integrationTest --parallel --max-workers=10
	@echo "Integration tests complete"

fmt:
	@echo "Formatting code..."
	@./gradlew spotlessApply
	@echo "Format complete"

e2e:
	@echo "Running E2E tests..."
	@echo "NOTE: Requires Docker running (uses Testcontainers)"
	@./gradlew :floe-server:quarkusBuild -Dquarkus.package.type=fast-jar
	@docker build -t floe:local -f docker/floe-server/Dockerfile .
	@./gradlew e2eTest --parallel --max-workers=10
	@echo "E2E tests complete"

clean:
	@echo "Cleaning all docker compose..."
	@docker compose down -v 2>/dev/null || true
	@for dir in rest-catalog nessie polaris hms lakekeeper gravitino; do \
		(cd examples/$$dir && FLOE_IMAGE=$(FLOE_IMAGE) FLOE_LIVY_IMAGE=$(FLOE_LIVY_IMAGE) docker compose --profile trino --profile spark down -v 2>/dev/null) || true; \
	done
	@docker ps -q --filter "publish=9091" | xargs -r docker stop 2>/dev/null || true
	@# Remove orphaned floe containers
	@docker ps -aq --filter "name=floe" --filter "name=hms-" --filter "name=rest-" --filter "name=nessie-" --filter "name=polaris-" | xargs -r docker rm -f 2>/dev/null || true
	@echo "Clean complete."

# Minimal Floe setup
start:
	@echo "Starting Floe (in-memory storage, no catalog or engine)..."
	@echo ""
	@echo "For end-to-end examples with Spark/Trino:"
	@echo "  make example-rest       # REST Catalog"
	@echo "  make example-nessie     # Nessie"
	@echo "  make example-polaris    # Polaris"
	@echo "  make example-hms        # Hive Metastore"
	@echo "  make example-lakekeeper # Lakekeeper"
	@echo "  make example-gravitino  # Gravitino"
	@echo ""
	@FLOE_IMAGE=$(FLOE_IMAGE) FLOE_LIVY_IMAGE=$(FLOE_LIVY_IMAGE) docker compose up -d
	@echo "Waiting for Floe to be ready..."
	@sleep 5
	@echo ""
	@echo "Floe is running at http://localhost:9091"


# Stop all running examples
define stop_all
	@for dir in rest-catalog nessie polaris hms lakekeeper gravitino; do \
		(cd examples/$$dir && docker compose --profile trino --profile spark stop 2>/dev/null) || true; \
	done
endef

# Start example with Spark engine
# $(1) = display name, $(2) = directory, $(3) = FLOE_IMAGE, $(4) = FLOE_LIVY_IMAGE
define start_spark
	$(call stop_all)
	@echo "Starting $(1) with Spark..."
	@cd examples/$(2) && FLOE_IMAGE=$(3) FLOE_LIVY_IMAGE=$(4) docker compose --profile spark up -d
	@sleep 8
	@if [ "$(SKIP_DEMO)" != "1" ]; then \
		SPARK=$$(docker ps --format '{{.Names}}' | grep -E '\-spark$$' | head -1); \
		echo "Waiting for services to be ready..."; \
		sleep 10; \
		docker cp scripts/setup-demo-tables.py $$SPARK:/tmp/setup-demo-tables.py; \
		docker exec $$SPARK spark-submit --master 'local[*]' /tmp/setup-demo-tables.py; \
		./scripts/setup-demo-policies.sh; \
	fi
	@echo ""
	@echo "Floe: http://localhost:9091"
endef

# Start example with Trino engine
# $(1) = display name, $(2) = directory, $(3) = FLOE_IMAGE, $(4) = FLOE_LIVY_IMAGE
define start_trino
	$(call stop_all)
	@echo "Starting $(1) with Trino..."
	@cd examples/$(2) && FLOE_IMAGE=$(3) FLOE_LIVY_IMAGE=$(4) FLOE_ENGINE_TYPE=TRINO docker compose --profile trino --profile spark up -d
	@sleep 8
	@if [ "$(SKIP_DEMO)" != "1" ]; then \
		SPARK=$$(docker ps --format '{{.Names}}' | grep -E '\-spark$$' | head -1); \
		echo "Waiting for services to be ready..."; \
		sleep 10; \
		docker cp scripts/setup-demo-tables.py $$SPARK:/tmp/setup-demo-tables.py; \
		docker exec $$SPARK spark-submit --master 'local[*]' /tmp/setup-demo-tables.py; \
		./scripts/setup-demo-policies.sh; \
	fi
	@echo ""
	@echo "Floe: http://localhost:9091"
endef

# Examples - REST Catalog

example-rest: example-rest-spark

example-rest-spark:
	$(call start_spark,REST Catalog,rest-catalog,$(FLOE_IMAGE),$(FLOE_LIVY_IMAGE))

example-rest-trino:
	$(call start_trino,REST Catalog,rest-catalog,$(FLOE_IMAGE),$(FLOE_LIVY_IMAGE))

example-rest-spark-local: build
	$(call start_spark,REST Catalog,rest-catalog,floe:local,floe-livy:local)

example-rest-trino-local: build
	$(call start_trino,REST Catalog,rest-catalog,floe:local,floe-livy:local)

# Examples - Nessie

example-nessie: example-nessie-spark

example-nessie-spark:
	$(call start_spark,Nessie,nessie,$(FLOE_IMAGE),$(FLOE_LIVY_IMAGE))

example-nessie-trino:
	$(call start_trino,Nessie,nessie,$(FLOE_IMAGE),$(FLOE_LIVY_IMAGE))

example-nessie-spark-local: build
	$(call start_spark,Nessie,nessie,floe:local,floe-livy:local)

example-nessie-trino-local: build
	$(call start_trino,Nessie,nessie,floe:local,floe-livy:local)

# Examples - Polaris

example-polaris: example-polaris-spark

example-polaris-spark:
	$(call start_spark,Polaris,polaris,$(FLOE_IMAGE),$(FLOE_LIVY_IMAGE))

example-polaris-trino:
	$(call start_trino,Polaris,polaris,$(FLOE_IMAGE),$(FLOE_LIVY_IMAGE))

example-polaris-spark-local: build
	$(call start_spark,Polaris,polaris,floe:local,floe-livy:local)

example-polaris-trino-local: build
	$(call start_trino,Polaris,polaris,floe:local,floe-livy:local)

# Examples - Hive Metastore

example-hms: example-hms-spark

example-hms-spark:
	$(call start_spark,Hive Metastore,hms,$(FLOE_IMAGE),$(FLOE_LIVY_IMAGE))

example-hms-trino:
	$(call start_trino,Hive Metastore,hms,$(FLOE_IMAGE),$(FLOE_LIVY_IMAGE))

example-hms-spark-local: build
	$(call start_spark,Hive Metastore,hms,floe:local,floe-livy:local)

example-hms-trino-local: build
	$(call start_trino,Hive Metastore,hms,floe:local,floe-livy:local)

# Examples - Lakekeeper

example-lakekeeper: example-lakekeeper-spark

example-lakekeeper-spark:
	$(call start_spark,Lakekeeper,lakekeeper,$(FLOE_IMAGE),$(FLOE_LIVY_IMAGE))

example-lakekeeper-trino:
	$(call start_trino,Lakekeeper,lakekeeper,$(FLOE_IMAGE),$(FLOE_LIVY_IMAGE))

example-lakekeeper-spark-local: build
	$(call start_spark,Lakekeeper,lakekeeper,floe:local,floe-livy:local)

example-lakekeeper-trino-local: build
	$(call start_trino,Lakekeeper,lakekeeper,floe:local,floe-livy:local)

# Examples - Gravitino

example-gravitino: example-gravitino-spark

example-gravitino-spark:
	$(call start_spark,Gravitino,gravitino,$(FLOE_IMAGE),$(FLOE_LIVY_IMAGE))

example-gravitino-trino:
	$(call start_trino,Gravitino,gravitino,$(FLOE_IMAGE),$(FLOE_LIVY_IMAGE))

example-gravitino-spark-local: build
	$(call start_spark,Gravitino,gravitino,floe:local,floe-livy:local)

example-gravitino-trino-local: build
	$(call start_trino,Gravitino,gravitino,floe:local,floe-livy:local)
