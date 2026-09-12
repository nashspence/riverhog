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

<a id="s-f0ad7b7550cd"></a>
| Concern | Contract |
|---|---|
| <a id="s-3615d71d056a"></a>`application` | riverhog |
| <a id="s-d0b2788d6de1"></a>`classification` | client-only-primitive |
| <a id="s-9145adea349a"></a>`cli_commands` | ["collection upload start"] |
| <a id="s-9d8f93e10f53"></a>`client` | ApiClient |
| <a id="s-702d48236724"></a>`method` | POST |
| <a id="s-482cf49991b4"></a>`operation_id` | add_collection_upload_session_tags |
| <a id="s-ce06b2b03672"></a>`path` | /v1/collection-upload-sessions/{collection_id}/tags |
| <a id="s-5edf08e682be"></a>`provider_evidence` | None |
| <a id="s-3935270e69fe"></a>`read_collection` | None |
| <a id="s-1bf5789e08b6"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/tags](../http/post-v1-collection-upload-sessions-collection-id-tags.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Governing policies

- <a id="pa-6a78c6ae05a1"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-bf986f73cc6f"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-8af51b0425df"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

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
