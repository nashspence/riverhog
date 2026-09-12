# Operation parity: request_collection_provenance_verification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-request-collection-prove-38819872a8:08264aff28 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collections` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/85`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/collections/{collection_id}/provenance/verification](../http/post-v1-collections-collection-id-provenance-verification.md)
- [piggity collection provenance verify](../../piggity/cli/piggity-collection-provenance-verify.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["collection provenance verify"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | request_collection_provenance_verification |
| `path` | /v1/collections/{collection_id}/provenance/verification |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fc8ebcc2d2b90338856ba0d924801d0a9a8a926fcf94eb12ce30e07ecea7f9a5 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection provenance verify"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "request_collection_provenance_verification",
  "path": "/v1/collections/{collection_id}/provenance/verification",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
