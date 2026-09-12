# Operation parity: step_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-step-evaluation:5c472d344c -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `evaluations` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/128`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/evaluations/{evaluation_id}/step](../http/post-v1-evaluations-evaluation-id-step.md)
- [stove0 evaluation step](../cli/stove0-evaluation-step.md)

## Contract

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
