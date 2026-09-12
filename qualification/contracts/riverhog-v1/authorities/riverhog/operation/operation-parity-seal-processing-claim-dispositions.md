# Operation parity: seal_processing_claim_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-seal-processing-claim-dispositions:4d5a928287 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-processing-claims` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/36`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/derivation/seal](../http/post-v1-collection-processing-claims-claim-id-derivation-seal.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | [] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | seal_processing_claim_dispositions |
| `path` | /v1/collection-processing-claims/{claim_id}/derivation/seal |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | canonical-document |
