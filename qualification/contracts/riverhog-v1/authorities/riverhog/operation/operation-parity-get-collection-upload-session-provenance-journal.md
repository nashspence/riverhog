# Operation parity: get_collection_upload_session_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-collection-upload-se-2a26b33bc5:823e3c51b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b859a5bfc6"></a>
| Concern | Contract |
|---|---|
| <a id="s-2895399f44"></a>`application` | riverhog |
| <a id="s-fbfb428569"></a>`classification` | client-only-primitive |
| <a id="s-aba734047a"></a>`cli_commands` | [] |
| <a id="s-3bffda9d7f"></a>`client` | ApiClient |
| <a id="s-39c3f9e57a"></a>`method` | GET |
| <a id="s-a8a2ca4d0a"></a>`operation_id` | get_collection_upload_session_provenance_journal |
| <a id="s-581ad2f6bb"></a>`path` | /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id} |
| <a id="s-ae0e5e8563"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-522ed263e4"></a>`read_collection` | None |
| <a id="s-19db4dc243"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}](../http/get-v1-collection-upload-sessions-collection-id-provenance-journals-journal-id.md)

## Governing policies

- <a id="pa-f5e54a22cf"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-0748bf9892"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-79520026b0"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/61`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d05d4c5db4373d6d9222b0e299ccae974380fdc6bc6d913959ec17d79fdd8f3f -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_collection_upload_session_provenance_journal",
  "path": "/v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
