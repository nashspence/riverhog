# Operation parity: scheduler_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-scheduler-status:f8b1a09d22 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `admin` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/116`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/admin/scheduler](../http/get-v1-admin-scheduler.md)
- [stove0 scheduler status](../cli/stove0-scheduler-status.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["scheduler status"] |
| `client` | Stove0ApiClient |
| `method` | GET |
| `operation_id` | scheduler_status |
| `path` | /v1/admin/scheduler |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |
