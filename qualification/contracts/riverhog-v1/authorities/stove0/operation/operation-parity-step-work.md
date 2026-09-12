# Operation parity: step_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-step-work:9bce803a3e -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `work` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/145`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/work/{work_id}/step](../http/post-v1-work-work-id-step.md)
- [stove0 work step](../cli/stove0-work-step.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["work step"] |
| `client` | Stove0ApiClient |
| `method` | POST |
| `operation_id` | step_work |
| `path` | /v1/work/{work_id}/step |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |
