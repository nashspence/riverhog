# Operation parity: retry_evaluation_variant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-retry-evaluation-variant:17f3defd1b -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `evaluations` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/129`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/evaluations/{evaluation_id}/variants/{variant_id}/retry](../http/post-v1-evaluations-evaluation-id-variants-variant-id-retry.md)
- [stove0 evaluation retry](../cli/stove0-evaluation-retry.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["evaluation retry"] |
| `client` | Stove0ApiClient |
| `method` | POST |
| `operation_id` | retry_evaluation_variant |
| `path` | /v1/evaluations/{evaluation_id}/variants/{variant_id}/retry |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7613e8088d67c3277d778725d7d1672ecc5d58cd16982ea43ea68b5abb3a85c9 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "evaluation retry"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "retry_evaluation_variant",
  "path": "/v1/evaluations/{evaluation_id}/variants/{variant_id}/retry",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
