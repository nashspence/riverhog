# Operation parity: collection_contains_tag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-collection-contains-tag:2a70c994a5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8b909372c5"></a>
| Concern | Contract |
|---|---|
| <a id="s-4bd21a4f5b"></a>`application` | riverhog |
| <a id="s-96ca870f8c"></a>`classification` | human-cli+json |
| <a id="s-028825eec1"></a>`cli_commands` | ["collection tag contains"] |
| <a id="s-fd8a147b39"></a>`client` | ApiClient |
| <a id="s-9d1710b0c9"></a>`method` | GET |
| <a id="s-b368a081ab"></a>`operation_id` | collection_contains_tag |
| <a id="s-36d98dccac"></a>`path` | /v1/collections/{collection_id}/tags:contains |
| <a id="s-5554e3a104"></a>`provider_evidence` | None |
| <a id="s-0be3b2095b"></a>`read_collection` | None |
| <a id="s-9b7a1de0c2"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/tags:contains](../http/get-v1-collections-collection-id-tags-contains.md)
- [piggity collection tag contains](../../piggity/cli/piggity-collection-tag-contains.md)

## Governing policies

- <a id="pa-6b1e79ae83"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-15090f2a03"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-6355d05f47"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/88`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 666571d1cacf9d4490a0982374a5c878043ad7cf37fee56a573c23b7db847cb3 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection tag contains"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "collection_contains_tag",
  "path": "/v1/collections/{collection_id}/tags:contains",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
