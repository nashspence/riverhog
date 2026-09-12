# Operation parity: add_collection_upload_session_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-add-collection-upload-session-tags:7b930443b0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f0ad7b7550"></a>
| Concern | Contract |
|---|---|
| <a id="s-3615d71d05"></a>`application` | riverhog |
| <a id="s-d0b2788d6d"></a>`classification` | client-only-primitive |
| <a id="s-9145adea34"></a>`cli_commands` | ["collection upload start"] |
| <a id="s-9d8f93e10f"></a>`client` | ApiClient |
| <a id="s-702d482367"></a>`method` | POST |
| <a id="s-482cf49991"></a>`operation_id` | add_collection_upload_session_tags |
| <a id="s-ce06b2b036"></a>`path` | /v1/collection-upload-sessions/{collection_id}/tags |
| <a id="s-5edf08e682"></a>`provider_evidence` | None |
| <a id="s-3935270e69"></a>`read_collection` | None |
| <a id="s-1bf5789e08"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/tags](../http/post-v1-collection-upload-sessions-collection-id-tags.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Governing policies

- <a id="pa-6a78c6ae05"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-bf986f73cc"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-8af51b0425"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/66`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2699f8ebb53c86d34de0b6a611e14a16242c82a224b43bb110d968ee28e08520 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "collection upload start"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "add_collection_upload_session_tags",
  "path": "/v1/collection-upload-sessions/{collection_id}/tags",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
