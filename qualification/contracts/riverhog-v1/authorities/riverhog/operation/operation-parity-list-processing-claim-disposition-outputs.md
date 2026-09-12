# Operation parity: list_processing_claim_disposition_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-processing-claim-di-7d7e9ee32e:62ff06b8e1 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-processing-claims` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/34`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/collection-processing-claims/{claim_id}/derivation/output-edges](../http/get-v1-collection-processing-claims-claim-id-derivation-output-edges.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | [] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | list_processing_claim_disposition_outputs |
| `path` | /v1/collection-processing-claims/{claim_id}/derivation/output-edges |
| `provider_evidence` | None |
| `read_collection` | {"authority": "processing-claim-disposition-outputs", "authority_parameter": "authority_sha256", "cursor_parameter": "start_ordinal", "fixed_limit": 128, "kind": "exact-authority-page"} |
| `response_authority` | canonical-document |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: da28414a08f2fe8c6c158a7fc98077f759532eef1084a9e3732e88ef7810f7ec -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_processing_claim_disposition_outputs",
  "path": "/v1/collection-processing-claims/{claim_id}/derivation/output-edges",
  "provider_evidence": null,
  "read_collection": {
    "authority": "processing-claim-disposition-outputs",
    "authority_parameter": "authority_sha256",
    "cursor_parameter": "start_ordinal",
    "fixed_limit": 128,
    "kind": "exact-authority-page"
  },
  "response_authority": "canonical-document"
}
```
