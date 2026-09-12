# Operation parity: acquire_collection_upload_session_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-acquire-collection-uploa-37b8920762:66e4942fd7 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-upload-sessions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/69`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}/work](../http/get-v1-collection-upload-sessions-collection-id-work.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | ["collection upload start"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | acquire_collection_upload_session_work |
| `path` | /v1/collection-upload-sessions/{collection_id}/work |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | canonical-document |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1f7e5d99e65daeeeb760bc150b95325f1d1efcf14483997108c4bdbf1bfee19d -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "collection upload start"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "acquire_collection_upload_session_work",
  "path": "/v1/collection-upload-sessions/{collection_id}/work",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
