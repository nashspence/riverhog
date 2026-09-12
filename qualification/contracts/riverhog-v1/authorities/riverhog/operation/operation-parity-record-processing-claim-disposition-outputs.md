# Operation parity: record_processing_claim_disposition_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-record-processing-claim-b22805a2c3:749200f58e -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-processing-claims` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/35`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [PUT /v1/collection-processing-claims/{claim_id}/derivation/output-edges](../http/put-v1-collection-processing-claims-claim-id-derivation-output-edges.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | [] |
| `client` | ApiClient |
| `method` | PUT |
| `operation_id` | record_processing_claim_disposition_outputs |
| `path` | /v1/collection-processing-claims/{claim_id}/derivation/output-edges |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | canonical-document |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cbb3219d34095c19a74f68b9500b08a01f8ea43c2bc564a3de783f123ad9fec8 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "PUT",
  "operation_id": "record_processing_claim_disposition_outputs",
  "path": "/v1/collection-processing-claims/{claim_id}/derivation/output-edges",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
