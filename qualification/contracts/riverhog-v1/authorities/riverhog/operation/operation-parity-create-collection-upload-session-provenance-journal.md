# Operation parity: create_collection_upload_session_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-create-collection-upload-9989d7c2e6:a2b269812c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-cee51fd4a6"></a>
| Concern | Contract |
|---|---|
| <a id="s-80489147d0"></a>`application` | riverhog |
| <a id="s-9c2bdc6a4f"></a>`classification` | client-only-primitive |
| <a id="s-6dc3c12b41"></a>`cli_commands` | [] |
| <a id="s-ad0aab248a"></a>`client` | ApiClient |
| <a id="s-cf71afda16"></a>`method` | PUT |
| <a id="s-d883e75fe1"></a>`operation_id` | create_collection_upload_session_provenance_journal |
| <a id="s-d511febedd"></a>`path` | /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id} |
| <a id="s-5f8e26bb2e"></a>`provider_evidence` | None |
| <a id="s-9330a89d27"></a>`read_collection` | None |
| <a id="s-b63b477484"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [PUT /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}](../http/put-v1-collection-upload-sessions-collection-id-provenance-journals-journal-id.md)

## Governing policies

- <a id="pa-28632e6bf0"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-f206220e48"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-a5e66d0f04"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/63`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d1e231932e780859c4574009ba5b766e4d4a89f16b15be95a99b158ebcaa4f64 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "PUT",
  "operation_id": "create_collection_upload_session_provenance_journal",
  "path": "/v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
