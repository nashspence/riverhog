# Operation parity: restart_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-restart-processing-claim:d70ac70bb1 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-processing-claims` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/48`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/restart](../http/post-v1-collection-processing-claims-claim-id-restart.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | [] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | restart_processing_claim |
| `path` | /v1/collection-processing-claims/{claim_id}/restart |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | canonical-document |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75518d0def5f25c75d6d68bc99d11d78a460518e712296683416dd86248e60ce -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "restart_processing_claim",
  "path": "/v1/collection-processing-claims/{claim_id}/restart",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
