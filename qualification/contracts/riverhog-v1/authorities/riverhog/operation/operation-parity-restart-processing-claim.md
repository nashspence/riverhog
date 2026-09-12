# Operation parity: restart_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-restart-processing-claim:d70ac70bb1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-286a68c6af"></a>
| Concern | Contract |
|---|---|
| <a id="s-d877d97a84"></a>`application` | riverhog |
| <a id="s-2f5263b364"></a>`classification` | client-only-primitive |
| <a id="s-e0b9c14bab"></a>`cli_commands` | [] |
| <a id="s-605988d8b0"></a>`client` | ApiClient |
| <a id="s-36a4694243"></a>`method` | POST |
| <a id="s-3996a9eb61"></a>`operation_id` | restart_processing_claim |
| <a id="s-3467176087"></a>`path` | /v1/collection-processing-claims/{claim_id}/restart |
| <a id="s-6f0ea9e71b"></a>`provider_evidence` | None |
| <a id="s-262e38be13"></a>`read_collection` | None |
| <a id="s-aee526c4ab"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/restart](../http/post-v1-collection-processing-claims-claim-id-restart.md)

## Governing policies

- <a id="pa-4671f596e3"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-401c7bec84"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-0d4c3e7fbd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/48`

### Exact owned JSON

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
