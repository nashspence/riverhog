# Operation parity: list_processing_claim_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-processing-claim-artifacts:3206799568 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-02347d6c61"></a>
| Concern | Contract |
|---|---|
| <a id="s-9998350f2c"></a>`application` | riverhog |
| <a id="s-5424e77f4b"></a>`classification` | client-only-primitive |
| <a id="s-817608d1e7"></a>`cli_commands` | [] |
| <a id="s-4af2f3f57c"></a>`client` | ApiClient |
| <a id="s-627707c20d"></a>`method` | GET |
| <a id="s-f3d563c129"></a>`operation_id` | list_processing_claim_artifacts |
| <a id="s-67d80c4525"></a>`path` | /v1/collection-processing-claims/{claim_id}/plan/artifacts |
| <a id="s-552682751e"></a>`provider_evidence` | None |
| <a id="s-4ced5257e4"></a>`read_collection` | {"authority": "processing-claim-artifacts", "authority_parameter": "authority_sha256", "cursor_parameter": "start_ordinal", "fixed_limit": 128, "kind": "exact-authority-page"} |
| <a id="s-2a48a229d0"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims/{claim_id}/plan/artifacts](../http/get-v1-collection-processing-claims-claim-id-plan-artifacts.md)

## Governing policies

- <a id="pa-ddc11d2155"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-8a4de75c06"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-e604620ea3"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/43`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6547ebf35fcf75c99ace1203b7c06171eba5b975fc208b3a5a2f236aab9a66f4 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_processing_claim_artifacts",
  "path": "/v1/collection-processing-claims/{claim_id}/plan/artifacts",
  "provider_evidence": null,
  "read_collection": {
    "authority": "processing-claim-artifacts",
    "authority_parameter": "authority_sha256",
    "cursor_parameter": "start_ordinal",
    "fixed_limit": 128,
    "kind": "exact-authority-page"
  },
  "response_authority": "canonical-document"
}
```
