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

<a id="s-2054d9a49d"></a>
| Concern | Contract |
|---|---|
| <a id="s-5bca49e2e6"></a>`application` | stove0 |
| <a id="s-f3e39e7b86"></a>`classification` | human-cli+json |
| <a id="s-473ad4494d"></a>`cli_commands` | ["preview"] |
| <a id="s-0b430fc129"></a>`client` | Stove0ApiClient |
| <a id="s-0ef00a7290"></a>`method` | POST |
| <a id="s-f64dab11d9"></a>`operation_id` | preview_workflow |
| <a id="s-28c2fc2739"></a>`path` | /v1/workflow-previews |
| <a id="s-bed93bfd29"></a>`provider_evidence` | None |
| <a id="s-59280d324a"></a>`read_collection` | None |
| <a id="s-98b4c9eb5d"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/workflow-previews](../http/post-v1-workflow-previews.md)
- [stove0 preview](../cli/stove0-preview.md)

## Governing policies

- <a id="pa-10ee18797c"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-cd0078d3f3"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-1cdf4dff70"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

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
