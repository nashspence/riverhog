# Operation parity: retry_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-retry-work:aaa9396625 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `work` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/144`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/work/{work_id}/retry](../http/post-v1-work-work-id-retry.md)
- [stove0 work retry](../cli/stove0-work-retry.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["work retry"] |
| `client` | Stove0ApiClient |
| `method` | POST |
| `operation_id` | retry_work |
| `path` | /v1/work/{work_id}/retry |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |
