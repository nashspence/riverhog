# Operation parity: seal_processing_claim_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-seal-processing-claim-artifacts:4cd01bef5e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8294519fee"></a>
| Concern | Contract |
|---|---|
| <a id="s-40db9da7cc"></a>`application` | riverhog |
| <a id="s-e5105075be"></a>`classification` | client-only-primitive |
| <a id="s-a6e1b91ff3"></a>`cli_commands` | [] |
| <a id="s-b41ae253fa"></a>`client` | ApiClient |
| <a id="s-fa00bbb53f"></a>`method` | POST |
| <a id="s-1d845407ef"></a>`operation_id` | seal_processing_claim_artifacts |
| <a id="s-98f8a3e156"></a>`path` | /v1/collection-processing-claims/{claim_id}/plan/artifacts/seal |
| <a id="s-0aafefebd7"></a>`provider_evidence` | None |
| <a id="s-ce438ef0fd"></a>`read_collection` | None |
| <a id="s-175305c768"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/plan/artifacts/seal](../http/post-v1-collection-processing-claims-claim-id-plan-artifacts-seal.md)

## Governing policies

- <a id="pa-2e73a4440c"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-d0d0ab5c57"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-f73da71af2"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/45`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 87f072f10d04ecce61417b70878a1abb5cbdcd6ad3a35ab2c5b75460b7b05f04 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "seal_processing_claim_artifacts",
  "path": "/v1/collection-processing-claims/{claim_id}/plan/artifacts/seal",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
