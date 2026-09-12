# Operation parity: health_ready

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-health-ready:0b492527d9 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `health` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/115`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /health/ready](../http/get-health-ready.md)
- [stove0 health](../cli/stove0-health.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | standard-tool/protocol |
| `cli_commands` | ["health"] |
| `client` | Stove0ApiClient |
| `method` | GET |
| `operation_id` | health_ready |
| `path` | /health/ready |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 60cebda6865bee636c94554b29e742fda4bde6bbfa1b36a6bf7cbd8b0fa51fd9 -->

```json
{
  "application": "stove0",
  "classification": "standard-tool/protocol",
  "cli_commands": [
    "health"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "health_ready",
  "path": "/health/ready",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
