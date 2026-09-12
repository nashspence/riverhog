# Operation parity: cancel_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-cancel-work:0d5f400798 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `work` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/142`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/work/{work_id}/cancel](../http/post-v1-work-work-id-cancel.md)
- [stove0 work cancel](../cli/stove0-work-cancel.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["work cancel"] |
| `client` | Stove0ApiClient |
| `method` | POST |
| `operation_id` | cancel_work |
| `path` | /v1/work/{work_id}/cancel |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |
