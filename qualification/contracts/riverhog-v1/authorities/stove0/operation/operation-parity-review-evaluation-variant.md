# Operation parity: review_evaluation_variant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-review-evaluation-variant:1a5900cfb5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `evaluations` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["evaluation review"] |
| `client` | Stove0ApiClient |
| `method` | PUT |
| `operation_id` | review_evaluation_variant |
| `path` | /v1/evaluations/{evaluation_id}/variants/{variant_id}/review |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [PUT /v1/evaluations/{evaluation_id}/variants/{variant_id}/review](../http/put-v1-evaluations-evaluation-id-variants-variant-id-review.md)
- [stove0 evaluation review](../cli/stove0-evaluation-review.md)

## Governing policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/130`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bbf7a2af7e628242a794efe06f244899ed65da54ff8dc1707314d500e8eecd6c -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "evaluation review"
  ],
  "client": "Stove0ApiClient",
  "method": "PUT",
  "operation_id": "review_evaluation_variant",
  "path": "/v1/evaluations/{evaluation_id}/variants/{variant_id}/review",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
