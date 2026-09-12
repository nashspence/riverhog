# Operation parity: list_collection_upload_session_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-collection-upload-s-52989231a3:747de2336f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-adadd2ac02"></a>
| Concern | Contract |
|---|---|
| <a id="s-fca688a584"></a>`application` | riverhog |
| <a id="s-e911f8bcbe"></a>`classification` | human-cli+json |
| <a id="s-66adc57fc4"></a>`cli_commands` | ["collection upload files"] |
| <a id="s-cf323a56c5"></a>`client` | ApiClient |
| <a id="s-9510066c9b"></a>`method` | GET |
| <a id="s-1011429c28"></a>`operation_id` | list_collection_upload_session_files |
| <a id="s-fcb8324995"></a>`path` | /v1/collection-upload-sessions/{collection_id}/files |
| <a id="s-b380c77996"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-f5e56bc382"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-195c09ef9b"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}/files](../http/get-v1-collection-upload-sessions-collection-id-files.md)
- [piggity collection upload files](../../piggity/cli/piggity-collection-upload-files.md)

## Governing policies

- <a id="pa-81e6302591"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-ce3021f000"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-7ea6022a72"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/58`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 77fe3c885dd5cfe87c0a255c4b9bf8344c97f379e33fb40c07e5d350e4d9b502 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection upload files"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_collection_upload_session_files",
  "path": "/v1/collection-upload-sessions/{collection_id}/files",
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
