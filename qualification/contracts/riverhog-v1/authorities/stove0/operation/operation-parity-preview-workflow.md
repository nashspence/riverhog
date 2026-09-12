# Operation parity: preview_workflow

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-preview-workflow:e296abd126 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [workflow-previews](families/workflow-previews/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-2054d9a49dac"></a>
| Concern | Contract |
|---|---|
| <a id="s-5bca49e2e656"></a>`application` | stove0 |
| <a id="s-f3e39e7b86be"></a>`classification` | human-cli+json |
| <a id="s-473ad4494d21"></a>`cli_commands` | ["preview"] |
| <a id="s-0b430fc12927"></a>`client` | Stove0ApiClient |
| <a id="s-0ef00a729005"></a>`method` | POST |
| <a id="s-f64dab11d9e5"></a>`operation_id` | preview_workflow |
| <a id="s-28c2fc2739dc"></a>`path` | /v1/workflow-previews |
| <a id="s-bed93bfd298a"></a>`provider_evidence` | None |
| <a id="s-59280d324a12"></a>`read_collection` | None |
| <a id="s-98b4c9eb5d0c"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/workflow-previews](../http/post-v1-workflow-previews.md)
- [stove0 preview](../cli/stove0-preview.md)

## Governing policies

- <a id="pa-10ee18797c40"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-cd0078d3f3f1"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-1cdf4dff7058"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

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
