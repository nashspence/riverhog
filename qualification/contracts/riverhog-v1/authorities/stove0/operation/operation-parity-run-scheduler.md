# Operation parity: run_scheduler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-run-scheduler:11193d6199 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `admin` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/117`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/admin/scheduler/run](../http/post-v1-admin-scheduler-run.md)
- [stove0 scheduler run](../cli/stove0-scheduler-run.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["scheduler run"] |
| `client` | Stove0ApiClient |
| `method` | POST |
| `operation_id` | run_scheduler |
| `path` | /v1/admin/scheduler/run |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |
