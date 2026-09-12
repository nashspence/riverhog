# Operation parity: list_collection_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-collection-tags:d37d7bb77f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b689bae7a9"></a>
| Concern | Contract |
|---|---|
| <a id="s-b7270faa82"></a>`application` | riverhog |
| <a id="s-a9e2b198ff"></a>`classification` | human-cli+json |
| <a id="s-3f056837a5"></a>`cli_commands` | ["collection tag list", "local add", "local repair", "local sync"] |
| <a id="s-f37b65b83a"></a>`client` | ApiClient |
| <a id="s-3b9275d042"></a>`method` | GET |
| <a id="s-8cdcd00e16"></a>`operation_id` | list_collection_tags |
| <a id="s-d997a6ce40"></a>`path` | /v1/collections/{collection_id}/tags |
| <a id="s-af1a40a281"></a>`provider_evidence` | None |
| <a id="s-7fc44bd288"></a>`read_collection` | {"authority": "collection-tag-set", "authority_parameter": "tag_set_identity", "cursor_parameter": "page_token", "kind": "exact-authority-page", "limit_parameter": "page_size"} |
| <a id="s-e45cff59f0"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/tags](../http/get-v1-collections-collection-id-tags.md)
- [piggity collection tag list](../../piggity/cli/piggity-collection-tag-list.md)
- [piggity local add](../../piggity/cli/piggity-local-add.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Governing policies

- <a id="pa-59fe8b15a4"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-5c22955b2c"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-ef408da9a8"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/86`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 20990a57b375fcd0baecbbc019829538f5213f93eeef4f6b867969ee27f76de3 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection tag list",
    "local add",
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_collection_tags",
  "path": "/v1/collections/{collection_id}/tags",
  "provider_evidence": null,
  "read_collection": {
    "authority": "collection-tag-set",
    "authority_parameter": "tag_set_identity",
    "cursor_parameter": "page_token",
    "kind": "exact-authority-page",
    "limit_parameter": "page_size"
  },
  "response_authority": "http-json"
}
```
