# Operation parity: cancel_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-cancel-collection-upload-session:b4906f637f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-upload-sessions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

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

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/cancel](../http/post-v1-collection-upload-sessions-collection-id-cancel.md)
- [piggity collection upload cancel](../../piggity/cli/piggity-collection-upload-cancel.md)

## Governing policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/54`

### Exact owned JSON

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
