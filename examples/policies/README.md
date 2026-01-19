# Policy Examples

Example policy configurations for Floe.

## Files

| File | Description |
|------|-------------|
| `comprehensive-policy.json` | All available fields for reference |
| `minimal-policy.json` | Minimal required fields |

## Usage

```bash
# Create policy from file
curl -X POST http://localhost:9091/api/v1/policies \
  -H "Content-Type: application/json" \
  -d @examples/policies/minimal-policy.json
```

## Field Reference

See [Policies Documentation](../../docs/policies.md) for complete field reference.
