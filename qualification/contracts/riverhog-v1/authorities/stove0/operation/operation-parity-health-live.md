# Operation parity: health_live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-health-live:c75d673fef -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `health` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/114`

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
- [stove0 health](../cli/stove0-health.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | standard-tool/protocol |
| `cli_commands` | ["health"] |
| `client` | Stove0ApiClient |
| `method` | GET |
| `operation_id` | health_live |
| `path` | /health/live |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c3bb17dd1416005d0eedae12c18e64e483a0c55bcadd67848073db7c288008e5 -->

```json
{
  "application": "stove0",
  "classification": "standard-tool/protocol",
  "cli_commands": [
    "health"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "health_live",
  "path": "/health/live",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
