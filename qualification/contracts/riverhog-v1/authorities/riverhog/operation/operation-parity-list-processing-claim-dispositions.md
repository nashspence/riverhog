# Operation parity: list_processing_claim_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-processing-claim-dispositions:b4c2abe4af -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-processing-claims` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/32`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/collection-processing-claims/{claim_id}/derivation/dispositions](../http/get-v1-collection-processing-claims-claim-id-derivation-dispositions.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | [] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | list_processing_claim_dispositions |
| `path` | /v1/collection-processing-claims/{claim_id}/derivation/dispositions |
| `provider_evidence` | None |
| `read_collection` | {"authority": "processing-claim-dispositions", "authority_parameter": "authority_sha256", "cursor_parameter": "start_ordinal", "fixed_limit": 128, "kind": "exact-authority-page"} |
| `response_authority` | canonical-document |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5c0d362770b39c3da3012a18780e936fa9281df6495e90aff5254b178ef9f7a7 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_processing_claim_dispositions",
  "path": "/v1/collection-processing-claims/{claim_id}/derivation/dispositions",
  "provider_evidence": null,
  "read_collection": {
    "authority": "processing-claim-dispositions",
    "authority_parameter": "authority_sha256",
    "cursor_parameter": "start_ordinal",
    "fixed_limit": 128,
    "kind": "exact-authority-page"
  },
  "response_authority": "canonical-document"
}
```
