# Operation parity: list_collection_provenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-collection-provenance:ae395b0fc0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f5594ef17bfd"></a>
| Concern | Contract |
|---|---|
| <a id="s-d131af58e00a"></a>`application` | riverhog |
| <a id="s-1337620f73fd"></a>`classification` | human-cli+json |
| <a id="s-4d66aba58465"></a>`cli_commands` | ["collection provenance list"] |
| <a id="s-7b2794afec24"></a>`client` | ApiClient |
| <a id="s-8e040c929c97"></a>`method` | GET |
| <a id="s-fc9da5af8e98"></a>`operation_id` | list_collection_provenance |
| <a id="s-7b023105bddd"></a>`path` | /v1/collections/{collection_id}/provenance/files |
| <a id="s-2c30ae8b72af"></a>`provider_evidence` | None |
| <a id="s-12048f8b421d"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-1a15241ccdc3"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/files](../http/get-v1-collections-collection-id-provenance-files.md)
- [piggity collection provenance list](../../piggity/cli/piggity-collection-provenance-list.md)

## Governing policies

- <a id="pa-488bb5c4f316"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-c76c88730238"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-6a33f18fe6fd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/77`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: da66647341b673f6c2c239a0c7f09d538e8279d97b05292cc93c8a2d3b7d3942 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection provenance list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_collection_provenance",
  "path": "/v1/collections/{collection_id}/provenance/files",
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
