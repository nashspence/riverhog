# Operation parity: step_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-step-evaluation:5c472d344c -->

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
| `cli_commands` | ["evaluation step"] |
| `client` | Stove0ApiClient |
| `method` | POST |
| `operation_id` | step_evaluation |
| `path` | /v1/evaluations/{evaluation_id}/step |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/evaluations/{evaluation_id}/step](../http/post-v1-evaluations-evaluation-id-step.md)
- [stove0 evaluation step](../cli/stove0-evaluation-step.md)

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

- `/external_contract/operations/128`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9156cd0b3ebbbcad96a357d21bb5a609b9ef213da79a21b4cd93c8b1288b9af8 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "evaluation step"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "step_evaluation",
  "path": "/v1/evaluations/{evaluation_id}/step",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
