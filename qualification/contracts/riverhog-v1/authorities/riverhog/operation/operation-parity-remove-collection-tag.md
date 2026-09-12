# Operation parity: remove_collection_tag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-remove-collection-tag:9e49a0970e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-82263f8bbed4"></a>
| Concern | Contract |
|---|---|
| <a id="s-59505c0686bc"></a>`application` | riverhog |
| <a id="s-f43f994df66d"></a>`classification` | human-cli+json |
| <a id="s-d66de586e553"></a>`cli_commands` | ["collection tag remove"] |
| <a id="s-ae0094ee1fde"></a>`client` | ApiClient |
| <a id="s-6fb71cbe950a"></a>`method` | POST |
| <a id="s-8e4ed795d549"></a>`operation_id` | remove_collection_tag |
| <a id="s-969e48d79c87"></a>`path` | /v1/collections/{collection_id}/tags:remove |
| <a id="s-edf780c8a97e"></a>`provider_evidence` | None |
| <a id="s-27be678be43a"></a>`read_collection` | None |
| <a id="s-1d8454bb679e"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collections/{collection_id}/tags:remove](../http/post-v1-collections-collection-id-tags-remove.md)
- [piggity collection tag remove](../../piggity/cli/piggity-collection-tag-remove.md)

## Governing policies

- <a id="pa-3a18441348cf"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-092eee147936"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-462a9e05e3c7"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/89`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d74063464c07f8a3706cacbfce144cc2c3e669aefb80e05239bb45ceb5e5a513 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection tag remove"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "remove_collection_tag",
  "path": "/v1/collections/{collection_id}/tags:remove",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
