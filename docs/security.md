# Security

Floe supports authentication and authorization to secure API access.

## Authentication

Two authentication methods are supported:

### API Keys

API keys are the primary authentication method for machine-to-machine access.

**Features:**
- SHA-256 hashed storage (plaintext never stored)
- Optional expiration dates
- Enable/disable without deletion
- Last-used tracking
- Prefixed format: `floe_<random-32-chars>`

**Usage:**
```bash
curl -H "X-API-Key: floe_abc123..." http://localhost:9091/api/v1/policies
```

**Configuration:**
```bash
# Enable authentication (default: true)
FLOE_AUTH_ENABLED=true

# Header name for API key (default: X-API-Key)
FLOE_AUTH_HEADER_NAME=X-API-Key

# Bootstrap key for initial setup (admin role)
FLOE_BOOTSTRAP_KEY=floe_your-initial-admin-key
```

### OIDC (OpenID Connect)

OIDC authentication for human users via identity providers (Keycloak, Auth0, Okta, Azure AD, AWS Cognito).

**Features:**
- JWT token validation (signature, expiration, issuer)
- Role extraction from multiple claim locations:
  - `roles` (standard)
  - `realm_access.roles` (Keycloak)
  - `permissions` (Auth0)
  - `groups` (Azure AD)
  - `scope` (OAuth2 scopes)
- Username extraction from `preferred_username`, `name`, `email`, or `sub`

**Usage:**
```bash
curl -H "Authorization: Bearer <jwt-token>" http://localhost:9091/api/v1/policies
```

**Configuration:**
```bash
# Enable OIDC (Quarkus OIDC configuration)
QUARKUS_OIDC_AUTH_SERVER_URL=https://keycloak.example.com/realms/floe
QUARKUS_OIDC_CLIENT_ID=floe
QUARKUS_OIDC_CLIENT_SECRET=your-secret

# Optional: explicit token validation
QUARKUS_OIDC_TOKEN_ISSUER=https://keycloak.example.com/realms/floe
QUARKUS_OIDC_TOKEN_AUDIENCE=floe
```

**Provider Priority:** OIDC (Bearer tokens) is checked before API keys when both are present.

## Authorization

Floe uses Role-Based Access Control (RBAC) with three built-in roles.

### Roles

| Role | Description | Permissions |
|------|-------------|-------------|
| `ADMIN` | Full system access | All permissions |
| `OPERATOR` | Trigger maintenance, view data | `READ_POLICIES`, `READ_TABLES`, `READ_OPERATIONS`, `TRIGGER_MAINTENANCE` |
| `VIEWER` | Read-only access | `READ_POLICIES`, `READ_TABLES`, `READ_OPERATIONS` |

### Permissions

| Permission | Description |
|------------|-------------|
| `READ_POLICIES` | View maintenance policies |
| `WRITE_POLICIES` | Create and update policies |
| `DELETE_POLICIES` | Delete policies |
| `READ_TABLES` | View tables and metadata |
| `READ_OPERATIONS` | View maintenance operation history |
| `TRIGGER_MAINTENANCE` | Trigger maintenance operations |
| `MANAGE_API_KEYS` | Create, update, and revoke API keys |

### Role Mapping

Map external identity provider roles to Floe roles:

```bash
# Built-in mappings (case-insensitive):
# admin, administrator, superuser, floe-admin -> ADMIN
# operator, maintainer, editor, floe-operator -> OPERATOR  
# viewer, readonly, read-only, reader, floe-viewer -> VIEWER

# Custom mappings:
FLOE_AUTH_ROLE_MAPPING_MY_CUSTOM_ADMIN=ADMIN
FLOE_AUTH_ROLE_MAPPING_DATA_ENGINEER=OPERATOR
```

## API Key Management

### Create Key

```bash
curl -X POST http://localhost:9091/api/v1/auth/keys \
  -H "X-API-Key: <admin-key>" \
  -H "Content-Type: application/json" \
  -d '{"name": "ci-pipeline", "role": "OPERATOR", "expiresAt": "2025-12-31T23:59:59Z"}'
```

Response includes the plaintext key (shown only once):
```json
{
  "key": "floe_abc123...",
  "id": "uuid",
  "name": "ci-pipeline",
  "role": "OPERATOR"
}
```

### List Keys

```bash
curl http://localhost:9091/api/v1/auth/keys \
  -H "X-API-Key: <admin-key>"
```

### Disable/Enable Key

```bash
curl -X PUT http://localhost:9091/api/v1/auth/keys/{id} \
  -H "X-API-Key: <admin-key>" \
  -H "Content-Type: application/json" \
  -d '{"enabled": false}'
```

### Delete Key

```bash
curl -X DELETE http://localhost:9091/api/v1/auth/keys/{id} \
  -H "X-API-Key: <admin-key>"
```

### Current User Info

```bash
curl http://localhost:9091/api/v1/auth/keys/me \
  -H "X-API-Key: <your-key>"
```

## Audit Logging

Security events are logged for compliance and monitoring.

**Events logged:**
- Authentication success/failure (invalid key, disabled key, expired key, invalid token)
- Authorization success/failure
- API key lifecycle (created, updated, revoked)
- Policy changes (created, updated, deleted)
- Maintenance operations (triggered, completed, failed)

**Configuration:**
```bash
# Enable audit logging (default: true)
FLOE_SECURITY_AUDIT_ENABLED=true

# Store in database (default: true)
FLOE_SECURITY_AUDIT_DATABASE_ENABLED=true

# Retention period in days (default: 2555 / ~7 years)
FLOE_SECURITY_AUDIT_RETENTION_DAYS=2555

# Archive to S3 (default: false)
FLOE_SECURITY_AUDIT_ARCHIVAL_ENABLED=false
FLOE_SECURITY_AUDIT_ARCHIVAL_THRESHOLD_DAYS=90
FLOE_SECURITY_AUDIT_ARCHIVAL_BUCKET=s3://audit-logs/floe/
```

Note: S3 archival is configured but not yet implemented; enabling it currently logs a warning and skips archival.

## Endpoint Protection

Endpoints are protected using the `@Secured` annotation with required permissions:

| Endpoint | Method | Permission |
|----------|--------|------------|
| `/api/v1/policies` | GET | `READ_POLICIES` |
| `/api/v1/policies` | POST | `WRITE_POLICIES` |
| `/api/v1/policies/{id}` | PUT | `WRITE_POLICIES` |
| `/api/v1/policies/{id}` | DELETE | `DELETE_POLICIES` |
| `/api/v1/tables/**` | GET | `READ_TABLES` |
| `/api/v1/operations/**` | GET | `READ_OPERATIONS` |
| `/api/v1/maintenance/trigger` | POST | `TRIGGER_MAINTENANCE` |
| `/api/v1/auth/keys` | GET, POST | `MANAGE_API_KEYS` |
| `/api/v1/auth/keys/{id}` | GET, PUT, DELETE | `MANAGE_API_KEYS` |
| `/api/v1/auth/keys/me` | GET | `READ_POLICIES` |
| `/api/v1/catalog` | GET | `READ_TABLES` |

## Custom Authorization Backends

Floe’s authorization is pluggable via the `AuthorizationProvider` interface. You can integrate external authz systems (e.g., OpenFGA, SpiceDB) or policy languages (e.g., Rego, Cedar) by implementing a custom provider and mapping decisions to Floe permissions.

## Disabling Authentication

For development or trusted environments:

```bash
FLOE_AUTH_ENABLED=false
```

When disabled, all endpoints are accessible without credentials.

## What IS Supported

- API key authentication with SHA-256 hashing
- OIDC/OAuth2 Bearer token authentication
- Role-based access control (ADMIN, OPERATOR, VIEWER)
- Custom role mapping from identity providers
- API key expiration and enable/disable
- Audit logging with configurable retention
- Multiple identity provider support (Keycloak, Auth0, Okta, Azure AD, Cognito)

## What is NOT Supported

- Multi-tenancy or namespace-level access control
- Fine-grained table-level permissions
- API key IP whitelisting (planned)
- API key rate limiting (planned)
- SAML authentication
- mTLS client certificate authentication
- External policy engines (OPA, Cedar)
