# Operation parity: list_collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-collections:c359e33746 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1a0ba6ccc8"></a>
| Concern | Contract |
|---|---|
| <a id="s-6865a05966"></a>`application` | riverhog |
| <a id="s-d3e145c2ec"></a>`classification` | human-cli+json |
| <a id="s-e81a3a16d6"></a>`cli_commands` | ["collection list"] |
| <a id="s-d7d22a0555"></a>`client` | ApiClient |
| <a id="s-e91af3349b"></a>`method` | GET |
| <a id="s-5093ce18dc"></a>`operation_id` | list_collections |
| <a id="s-03cd6dae74"></a>`path` | /v1/collections |
| <a id="s-036d6d22fb"></a>`provider_evidence` | None |
| <a id="s-0662f7ec58"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-904d5c96a3"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collections](../http/get-v1-collections.md)
- [piggity collection list](../../piggity/cli/piggity-collection-list.md)

## Governing policies

- <a id="pa-d5cb290640"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-f56926ef04"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-0ccc9f8f11"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/70`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 43956fb03cba594a6f52239cba169e87d2246a2390562c2e3c2da7a77580084e -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_collections",
  "path": "/v1/collections",
  "provider_evidence": null,
  "read_collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  },
  "response_authority": "http-json"
}
```
