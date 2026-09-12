# Operation parity: register_collection_upload_session_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-register-collection-uplo-131a48eee8:0a727af6f8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-bb005f76c6e7"></a>
| Concern | Contract |
|---|---|
| <a id="s-c2e2fc94e14a"></a>`application` | riverhog |
| <a id="s-d5ec1e58e1ab"></a>`classification` | client-only-primitive |
| <a id="s-76b3b39095db"></a>`cli_commands` | ["collection upload start"] |
| <a id="s-95664aea9b52"></a>`client` | ApiClient |
| <a id="s-4ac40f402cc5"></a>`method` | POST |
| <a id="s-fc0323363075"></a>`operation_id` | register_collection_upload_session_files |
| <a id="s-1c6bc1eb8758"></a>`path` | /v1/collection-upload-sessions/{collection_id}/files |
| <a id="s-68d2e3f8e867"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-54a2c95b71aa"></a>`read_collection` | None |
| <a id="s-a9ce23efd2d9"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/files](../http/post-v1-collection-upload-sessions-collection-id-files.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Governing policies

- <a id="pa-9854a837d163"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-aa443d399625"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-b0753897b413"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/59`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc0b31a46251fbf9cd9cd9b8adf357015a07f510c60c1835b733bf32e5aab090 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "collection upload start"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "register_collection_upload_session_files",
  "path": "/v1/collection-upload-sessions/{collection_id}/files",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
