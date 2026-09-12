# Operation parity: get_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-get-evaluation:21df6ac1b6 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `evaluations` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/126`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/evaluations/{evaluation_id}](../http/get-v1-evaluations-evaluation-id.md)
- [stove0 evaluation show](../cli/stove0-evaluation-show.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["evaluation show"] |
| `client` | Stove0ApiClient |
| `method` | GET |
| `operation_id` | get_evaluation |
| `path` | /v1/evaluations/{evaluation_id} |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d15c2996d1c311d94a76d209622f2054bd940647563ca3b1785e15150514a947 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "evaluation show"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "get_evaluation",
  "path": "/v1/evaluations/{evaluation_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
