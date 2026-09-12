# Operation parity: stream_collection_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-stream-collection-proven-322348bee0:8c445edec2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c1dfae8f0a"></a>
| Concern | Contract |
|---|---|
| <a id="s-4d55e9a82d"></a>`application` | riverhog |
| <a id="s-031f17d957"></a>`classification` | client-only-primitive |
| <a id="s-40c8f8d46f"></a>`cli_commands` | ["collection provenance export"] |
| <a id="s-3d17ca0a3d"></a>`client` | ApiClient |
| <a id="s-c3bbc9da94"></a>`method` | GET |
| <a id="s-4de1374662"></a>`operation_id` | stream_collection_provenance_journal |
| <a id="s-f6f1000346"></a>`path` | /v1/collections/{collection_id}/provenance/journals/{journal_id} |
| <a id="s-034c50a889"></a>`provider_evidence` | None |
| <a id="s-4442546854"></a>`read_collection` | None |
| <a id="s-e7787f2e33"></a>`response_authority` | stream-or-empty |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/journals/{journal_id}](../http/get-v1-collections-collection-id-provenance-journals-journal-id.md)
- [piggity collection provenance export](../../piggity/cli/piggity-collection-provenance-export.md)

## Governing policies

- <a id="pa-f098e32cbe"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-753b2541d5"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-14ffe16bfd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/79`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a1fcef17dc29af3e97eed898682e04a5042a304fe3185e8bb1d892e34061a8f -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "collection provenance export"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "stream_collection_provenance_journal",
  "path": "/v1/collections/{collection_id}/provenance/journals/{journal_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "stream-or-empty"
}
```
