# Operation parity: seal_processing_claim_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-seal-processing-claim-plan:1962a6c53b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6a52ba49ec0c"></a>
| Concern | Contract |
|---|---|
| <a id="s-2bb13a9ae0a1"></a>`application` | riverhog |
| <a id="s-0836d0a690c6"></a>`classification` | client-only-primitive |
| <a id="s-de882b84655f"></a>`cli_commands` | [] |
| <a id="s-0d6ba93795c0"></a>`client` | ApiClient |
| <a id="s-ca95e89a3061"></a>`method` | POST |
| <a id="s-2c3713a72493"></a>`operation_id` | seal_processing_claim_plan |
| <a id="s-a4e7040b5558"></a>`path` | /v1/collection-processing-claims/{claim_id}/plan |
| <a id="s-4421b360319d"></a>`provider_evidence` | None |
| <a id="s-c04ba2c2ab4f"></a>`read_collection` | None |
| <a id="s-c89c3af7c385"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/plan](../http/post-v1-collection-processing-claims-claim-id-plan.md)

## Governing policies

- <a id="pa-445b32135eef"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-a2296bc63655"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-0b77bb15aa6c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/42`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 73ca6d482edf094c01a84d94e2b77a52958530f7db6e6c1dc1429029036354f3 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "seal_processing_claim_plan",
  "path": "/v1/collection-processing-claims/{claim_id}/plan",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
