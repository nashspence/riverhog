# Operation parity: get_collection_upload_session_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-collection-upload-se-2a26b33bc5:823e3c51b9 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-upload-sessions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/61`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}](../http/get-v1-collection-upload-sessions-collection-id-provenance-journals-journal-id.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | [] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | get_collection_upload_session_provenance_journal |
| `path` | /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id} |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | canonical-document |

## Complete owned contract

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
