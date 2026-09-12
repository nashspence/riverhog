# Operation parity: list_collection_upload_sessions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-collection-upload-sessions:cb65abb4eb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7fe99f8937e6"></a>
| Concern | Contract |
|---|---|
| <a id="s-56095fa56a2b"></a>`application` | riverhog |
| <a id="s-09c366dcb4af"></a>`classification` | human-cli+json |
| <a id="s-17a89a5c401a"></a>`cli_commands` | ["collection upload list"] |
| <a id="s-4033a8f19ad2"></a>`client` | ApiClient |
| <a id="s-245a793b4b14"></a>`method` | GET |
| <a id="s-cb224fc8dc7a"></a>`operation_id` | list_collection_upload_sessions |
| <a id="s-24a6db57c8c5"></a>`path` | /v1/collection-upload-sessions |
| <a id="s-524ea8319a2c"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-8d52a4c9641b"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-662763ff7598"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-upload-sessions](../http/get-v1-collection-upload-sessions.md)
- [piggity collection upload list](../../piggity/cli/piggity-collection-upload-list.md)

## Governing policies

- <a id="pa-67b690c86a11"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-a4a2cc745a00"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-081fedf85c4e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/51`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6acd9ba99b0fc789dd84e9e5d8ac3e898f2285cfcd3a808a9a8e15a135206ef6 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection upload list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_collection_upload_sessions",
  "path": "/v1/collection-upload-sessions",
  "provider_evidence": "provider-qualification:#442",
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
