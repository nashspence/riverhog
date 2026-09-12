# Operation parity: health_live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-health-live:a8f70c3c13 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `health` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/0`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /health/live](../http/get-health-live.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | standard-tool/protocol |
| `cli_commands` | [] |
| `client` | None |
| `method` | GET |
| `operation_id` | health_live |
| `path` | /health/live |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 69366580c4dcb0ec633c62eabf8bd22f06606bd0b4b667c6b4675dade29fab98 -->

```json
{
  "application": "riverhog",
  "classification": "standard-tool/protocol",
  "cli_commands": [],
  "client": null,
  "method": "GET",
  "operation_id": "health_live",
  "path": "/health/live",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
