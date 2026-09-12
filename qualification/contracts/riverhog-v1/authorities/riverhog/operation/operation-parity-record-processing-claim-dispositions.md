# Operation parity: record_processing_claim_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-record-processing-claim-2a587c911d:c4f5a56f18 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-processing-claims` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/33`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [PUT /v1/collection-processing-claims/{claim_id}/derivation/dispositions](../http/put-v1-collection-processing-claims-claim-id-derivation-dispositions.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | [] |
| `client` | ApiClient |
| `method` | PUT |
| `operation_id` | record_processing_claim_dispositions |
| `path` | /v1/collection-processing-claims/{claim_id}/derivation/dispositions |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | canonical-document |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e9a940ae650b2d7df7e7735f9c2dbb62ef2f8a9898d96e5c9658ef1d0450266f -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "PUT",
  "operation_id": "record_processing_claim_dispositions",
  "path": "/v1/collection-processing-claims/{claim_id}/derivation/dispositions",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
