# Testing

## Running Tests Locally

```bash
# Unit tests only
make test

# Integration tests only (requires Docker)
make integration-test

# E2E tests (requires Docker)
make e2e
```

## Code Formatting

Floe uses Spotless with Google Java Format:

```bash
# Auto-fix formatting
make fmt
```

## CI

GitHub Actions runs on every PR:

```bash
# Runs unit and integration tests
./gradlew ciTest
# Runs end-to-end tests
./gradlew e2eTest
```
