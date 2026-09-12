# Operation parity: seal_processing_claim_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-seal-processing-claim-inputs:502088ef3f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c5c619a2aa12"></a>
| Concern | Contract |
|---|---|
| <a id="s-eb542781e974"></a>`application` | riverhog |
| <a id="s-b8b3b0ae46d6"></a>`classification` | client-only-primitive |
| <a id="s-bfaf1ff6dd41"></a>`cli_commands` | [] |
| <a id="s-b19675a060d7"></a>`client` | ApiClient |
| <a id="s-9b53994d6f35"></a>`method` | POST |
| <a id="s-92dad4b42ec9"></a>`operation_id` | seal_processing_claim_inputs |
| <a id="s-de44886821f9"></a>`path` | /v1/collection-processing-claims/{claim_id}/inputs/seal |
| <a id="s-e5c137dc1cf6"></a>`provider_evidence` | None |
| <a id="s-6ca3a6628b30"></a>`read_collection` | None |
| <a id="s-72ce957e653d"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/inputs/seal](../http/post-v1-collection-processing-claims-claim-id-inputs-seal.md)

## Governing policies

- <a id="pa-ce663cb79542"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-625b0b9b98d0"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-f6d7829e4531"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/39`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f992258e2a74ccf7d781cd0b0477c30fffc2ec769bb3879e01a0830a7aab5c45 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "seal_processing_claim_inputs",
  "path": "/v1/collection-processing-claims/{claim_id}/inputs/seal",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
