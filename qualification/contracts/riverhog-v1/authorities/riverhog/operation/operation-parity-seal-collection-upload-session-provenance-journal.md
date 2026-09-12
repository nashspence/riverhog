# Operation parity: seal_collection_upload_session_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-seal-collection-upload-s-761762d953:41f9a11661 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a378e23846d2"></a>
| Concern | Contract |
|---|---|
| <a id="s-969c4c2605d5"></a>`application` | riverhog |
| <a id="s-5485edf2b5e2"></a>`classification` | client-only-primitive |
| <a id="s-168637ff90ce"></a>`cli_commands` | [] |
| <a id="s-7164c9b5bdbb"></a>`client` | ApiClient |
| <a id="s-ad06ddc8ad72"></a>`method` | POST |
| <a id="s-7b0a595a8d4a"></a>`operation_id` | seal_collection_upload_session_provenance_journal |
| <a id="s-0223573f3d16"></a>`path` | /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}/seal |
| <a id="s-218eb4e6ff86"></a>`provider_evidence` | None |
| <a id="s-5e166bd0142c"></a>`read_collection` | None |
| <a id="s-7ce408df4993"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}/seal](../http/post-v1-collection-upload-sessions-collection-id-provenance-journals-journal-id-seal.md)

## Governing policies

- <a id="pa-1c00062b39aa"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-2fc36145a849"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-fea11c225206"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/64`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 23876f58801ea4afcf07ea9fb71f4e70efbf3f3ab730be9651c851512f2df84f -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "seal_collection_upload_session_provenance_journal",
  "path": "/v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}/seal",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
