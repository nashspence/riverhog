# Operation parity: discard_collection_upload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-discard-collection-upload:a66ff760ff -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-upload-sessions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/56`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/discard](../http/post-v1-collection-upload-sessions-collection-id-discard.md)
- [piggity collection upload discard](../../piggity/cli/piggity-collection-upload-discard.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["collection upload discard"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | discard_collection_upload |
| `path` | /v1/collection-upload-sessions/{collection_id}/discard |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 801ebb6126f094888c7f18d3b5a64afac285fd712928ec2ef6fee07f16dff411 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection upload discard"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "discard_collection_upload",
  "path": "/v1/collection-upload-sessions/{collection_id}/discard",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
