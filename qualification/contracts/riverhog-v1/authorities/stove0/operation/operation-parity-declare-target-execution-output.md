# Operation parity: declare_target_execution_output

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-declare-target-execution-output:3f5796943f -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `target-executions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/136`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [PUT /v1/target-executions/{job_id}/outputs/{artifact_id}](../http/put-v1-target-executions-job-id-outputs-artifact-id.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | client-only-primitive |
| `cli_commands` | [] |
| `client` | TargetCallbackClient |
| `method` | PUT |
| `operation_id` | declare_target_execution_output |
| `path` | /v1/target-executions/{job_id}/outputs/{artifact_id} |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | canonical-document |
