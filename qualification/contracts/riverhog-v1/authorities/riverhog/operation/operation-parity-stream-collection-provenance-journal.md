# Operation parity: stream_collection_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-stream-collection-proven-322348bee0:8c445edec2 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collections` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/79`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/collections/{collection_id}/provenance/journals/{journal_id}](../http/get-v1-collections-collection-id-provenance-journals-journal-id.md)
- [piggity collection provenance export](../../piggity/cli/piggity-collection-provenance-export.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | ["collection provenance export"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | stream_collection_provenance_journal |
| `path` | /v1/collections/{collection_id}/provenance/journals/{journal_id} |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | stream-or-empty |

## Complete owned contract

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
