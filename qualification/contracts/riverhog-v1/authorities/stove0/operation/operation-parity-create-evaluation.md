# Operation parity: create_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-create-evaluation:44546c282c -->

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
| `cli_commands` | ["evaluation create"] |
| `client` | Stove0ApiClient |
| `method` | POST |
| `operation_id` | create_evaluation |
| `path` | /v1/evaluations |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/evaluations](../http/post-v1-evaluations.md)
- [stove0 evaluation create](../cli/stove0-evaluation-create.md)

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

- `/external_contract/operations/125`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12e9bfc10407812b8facc18d4b76500a002fd71e033371806d2c33ebfb87cd85 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "evaluation create"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "create_evaluation",
  "path": "/v1/evaluations",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
