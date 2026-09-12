# Operation parity: begin_processing_claim_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-begin-processing-claim-retirement:ad264a88c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b22a71bf96"></a>
| Concern | Contract |
|---|---|
| <a id="s-49280e13c1"></a>`application` | riverhog |
| <a id="s-96e11f6494"></a>`classification` | client-only-primitive |
| <a id="s-df67693449"></a>`cli_commands` | [] |
| <a id="s-8a017af5db"></a>`client` | ApiClient |
| <a id="s-c97482729d"></a>`method` | POST |
| <a id="s-b987c79483"></a>`operation_id` | begin_processing_claim_retirement |
| <a id="s-8170486d2e"></a>`path` | /v1/collection-processing-claims/{claim_id}/retirement |
| <a id="s-0cb1a30117"></a>`provider_evidence` | None |
| <a id="s-a1ce0f4c4b"></a>`read_collection` | None |
| <a id="s-d434fd537b"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/retirement](../http/post-v1-collection-processing-claims-claim-id-retirement.md)

## Governing policies

- <a id="pa-0308b70e77"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-357376cb65"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-ace9e6e589"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/49`

### Exact owned JSON

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
