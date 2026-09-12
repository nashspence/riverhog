# Operation parity: begin_processing_claim_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-begin-processing-claim-retirement:ad264a88c2 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-processing-claims` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/49`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/retirement](../http/post-v1-collection-processing-claims-claim-id-retirement.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | [] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | begin_processing_claim_retirement |
| `path` | /v1/collection-processing-claims/{claim_id}/retirement |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | canonical-document |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 62a15d8c8c7db2022405e8bc3563719b9b0a11278dc6dc457b39a1a479eb4843 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "begin_processing_claim_retirement",
  "path": "/v1/collection-processing-claims/{claim_id}/retirement",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
