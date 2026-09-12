# Operation parity: add_collection_tag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-add-collection-tag:d1606a8eeb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b59d60590b"></a>
| Concern | Contract |
|---|---|
| <a id="s-84f62adceb"></a>`application` | riverhog |
| <a id="s-f60e4a47ea"></a>`classification` | human-cli+json |
| <a id="s-e581af9fff"></a>`cli_commands` | ["collection tag add"] |
| <a id="s-13e08c46e1"></a>`client` | ApiClient |
| <a id="s-59a8bcee3a"></a>`method` | POST |
| <a id="s-dcc8996613"></a>`operation_id` | add_collection_tag |
| <a id="s-4cbcdeb961"></a>`path` | /v1/collections/{collection_id}/tags:add |
| <a id="s-243e0e74a4"></a>`provider_evidence` | None |
| <a id="s-f3cd592223"></a>`read_collection` | None |
| <a id="s-4fda55e3f5"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collections/{collection_id}/tags:add](../http/post-v1-collections-collection-id-tags-add.md)
- [piggity collection tag add](../../piggity/cli/piggity-collection-tag-add.md)

## Governing policies

- <a id="pa-df8c386dd2"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-05815c9f01"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-9be9bcfa68"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/87`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9d9f0375da20a722b152ef6ef271d52014df79b1f3d848cadfd6ce3c6b2a6e59 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection tag add"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "add_collection_tag",
  "path": "/v1/collections/{collection_id}/tags:add",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
