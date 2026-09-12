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

<a id="s-b22a71bf9627"></a>
| Concern | Contract |
|---|---|
| <a id="s-49280e13c10c"></a>`application` | riverhog |
| <a id="s-96e11f6494b5"></a>`classification` | client-only-primitive |
| <a id="s-df6769344942"></a>`cli_commands` | [] |
| <a id="s-8a017af5dbeb"></a>`client` | ApiClient |
| <a id="s-c97482729d3d"></a>`method` | POST |
| <a id="s-b987c7948327"></a>`operation_id` | begin_processing_claim_retirement |
| <a id="s-8170486d2e7a"></a>`path` | /v1/collection-processing-claims/{claim_id}/retirement |
| <a id="s-0cb1a30117ef"></a>`provider_evidence` | None |
| <a id="s-a1ce0f4c4bf8"></a>`read_collection` | None |
| <a id="s-d434fd537bf7"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/retirement](../http/post-v1-collection-processing-claims-claim-id-retirement.md)

## Governing policies

- <a id="pa-0308b70e77e5"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-357376cb6576"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-ace9e6e5899b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

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
