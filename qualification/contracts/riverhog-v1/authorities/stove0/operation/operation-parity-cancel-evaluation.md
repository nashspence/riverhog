# Operation parity: cancel_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-cancel-evaluation:34c2bde7b9 -->

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
| `cli_commands` | ["evaluation cancel"] |
| `client` | Stove0ApiClient |
| `method` | POST |
| `operation_id` | cancel_evaluation |
| `path` | /v1/evaluations/{evaluation_id}/cancel |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/evaluations/{evaluation_id}/cancel](../http/post-v1-evaluations-evaluation-id-cancel.md)
- [stove0 evaluation cancel](../cli/stove0-evaluation-cancel.md)

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

- `/external_contract/operations/127`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c947b5a6f051a09bcb17da65b09eed2d1d4620b91c98f8feb9000fbb9430b744 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "evaluation cancel"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "cancel_evaluation",
  "path": "/v1/evaluations/{evaluation_id}/cancel",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
