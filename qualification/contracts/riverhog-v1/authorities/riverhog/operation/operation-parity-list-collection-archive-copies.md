# Operation parity: list_collection_archive_copies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-collection-archive-copies:9d45a4ee63 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-fa28e631ca"></a>
| Concern | Contract |
|---|---|
| <a id="s-d7b6859dbc"></a>`application` | riverhog |
| <a id="s-7c2ff6c508"></a>`classification` | human-cli+json |
| <a id="s-ea090698dc"></a>`cli_commands` | ["collection archive-copies"] |
| <a id="s-4ca047dc29"></a>`client` | ApiClient |
| <a id="s-b473fb56dc"></a>`method` | GET |
| <a id="s-b50dce87a4"></a>`operation_id` | list_collection_archive_copies |
| <a id="s-308c1374f1"></a>`path` | /v1/collections/{collection_id}/archive-copies |
| <a id="s-f15e8eb1ea"></a>`provider_evidence` | None |
| <a id="s-b5cb9bb5fc"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-395ba53b55"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/archive-copies](../http/get-v1-collections-collection-id-archive-copies.md)
- [piggity collection archive-copies](../../piggity/cli/piggity-collection-archive-copies.md)

## Governing policies

- <a id="pa-a2d96d8dcb"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-84fed4a971"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-392ffc54f5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/72`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad2566b36850f94f867261d76c6fbf08aa559f75034d949b2e250238d58dc247 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection archive-copies"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_collection_archive_copies",
  "path": "/v1/collections/{collection_id}/archive-copies",
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
