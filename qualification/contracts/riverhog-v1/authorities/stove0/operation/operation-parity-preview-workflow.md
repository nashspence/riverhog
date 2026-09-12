# Operation parity: preview_workflow

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-preview-workflow:e296abd126 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `workflow-previews` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/146`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/workflow-previews](../http/post-v1-workflow-previews.md)
- [stove0 preview](../cli/stove0-preview.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["preview"] |
| `client` | Stove0ApiClient |
| `method` | POST |
| `operation_id` | preview_workflow |
| `path` | /v1/workflow-previews |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | canonical-document |
