# Operation parity: append_collection_upload_session_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-append-collection-upload-a8a0e8d59a:5eba4e3958 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-344932ea85e7"></a>
| Concern | Contract |
|---|---|
| <a id="s-fe575c88c811"></a>`application` | riverhog |
| <a id="s-e03997810e75"></a>`classification` | client-only-primitive |
| <a id="s-50ef7338b1d1"></a>`cli_commands` | [] |
| <a id="s-8d1514621e65"></a>`client` | ApiClient |
| <a id="s-523792142fd2"></a>`method` | PATCH |
| <a id="s-4872fee7e8b8"></a>`operation_id` | append_collection_upload_session_provenance_journal |
| <a id="s-557d264b3d12"></a>`path` | /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id} |
| <a id="s-7002b943829c"></a>`provider_evidence` | None |
| <a id="s-1b15f1d7b113"></a>`read_collection` | None |
| <a id="s-a2f9cfbde09e"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [PATCH /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}](../http/patch-v1-collection-upload-sessions-collection-id-provenance-journals-journal-id.md)

## Governing policies

- <a id="pa-64e6175a38ba"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-19a09ccf58b4"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-1f7f4d44d8a6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/62`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 93dad3e2154fdea22d192449b6b028db1e16520c23de2f221a32f032bd374dae -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "PATCH",
  "operation_id": "append_collection_upload_session_provenance_journal",
  "path": "/v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
