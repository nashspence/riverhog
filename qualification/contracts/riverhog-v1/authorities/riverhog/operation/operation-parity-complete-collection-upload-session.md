# Operation parity: complete_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-complete-collection-upload-session:e6ea8da52b -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-upload-sessions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/55`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/complete](../http/post-v1-collection-upload-sessions-collection-id-complete.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | ["collection upload start"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | complete_collection_upload_session |
| `path` | /v1/collection-upload-sessions/{collection_id}/complete |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 73ed3b84d186ba2c91da879e0f6a73a63612295215ebe0b953b2badccc75df3e -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "collection upload start"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "complete_collection_upload_session",
  "path": "/v1/collection-upload-sessions/{collection_id}/complete",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
