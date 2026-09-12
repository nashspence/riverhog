# Operation parity: preview_workflow

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-preview-workflow:e296abd126 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `workflow-previews` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

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

## Maintained corroboration

### Related interface records

- [POST /v1/workflow-previews](../http/post-v1-workflow-previews.md)
- [stove0 preview](../cli/stove0-preview.md)

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

- `/external_contract/operations/146`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a08bfda804ba810ec6d11616c614ad0a1e35324e9a79c7c3cadc2c5b5b8ff213 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "preview"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "preview_workflow",
  "path": "/v1/workflow-previews",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
