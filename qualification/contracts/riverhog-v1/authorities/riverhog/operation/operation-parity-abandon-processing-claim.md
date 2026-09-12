# Operation parity: abandon_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-abandon-processing-claim:008f825ed3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-processing-claims` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | [] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | abandon_processing_claim |
| `path` | /v1/collection-processing-claims/{claim_id}/abandon |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/abandon](../http/post-v1-collection-processing-claims-claim-id-abandon.md)

## Governing policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/27`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fffb5468d6f227524aa437e1917af1750a0c30320d4bf6a937cd3ecfa7a24400 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "abandon_processing_claim",
  "path": "/v1/collection-processing-claims/{claim_id}/abandon",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
