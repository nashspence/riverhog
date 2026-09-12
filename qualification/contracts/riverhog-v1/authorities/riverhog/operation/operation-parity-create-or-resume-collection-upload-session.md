# Operation parity: create_or_resume_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-create-or-resume-collect-e3d1a894e1:90d65f8839 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-upload-sessions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/52`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/collection-upload-sessions](../http/post-v1-collection-upload-sessions.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | ["collection upload start"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | create_or_resume_collection_upload_session |
| `path` | /v1/collection-upload-sessions |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1b4e5ecfe060eb6e0885995677578313ed7b4d1b0c721083a36c560859f1466b -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "collection upload start"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "create_or_resume_collection_upload_session",
  "path": "/v1/collection-upload-sessions",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
