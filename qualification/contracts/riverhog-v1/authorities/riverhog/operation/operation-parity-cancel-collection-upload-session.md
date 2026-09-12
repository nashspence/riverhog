# Operation parity: cancel_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-cancel-collection-upload-session:b4906f637f -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-upload-sessions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/54`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/cancel](../http/post-v1-collection-upload-sessions-collection-id-cancel.md)
- [piggity collection upload cancel](../../piggity/cli/piggity-collection-upload-cancel.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["collection upload cancel"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | cancel_collection_upload_session |
| `path` | /v1/collection-upload-sessions/{collection_id}/cancel |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e2cf867f13b119a0942aa86c804878a7972ea61c79591ad74e8bccdb060787a -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection upload cancel"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "cancel_collection_upload_session",
  "path": "/v1/collection-upload-sessions/{collection_id}/cancel",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
