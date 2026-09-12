# Operation parity: get_processing_claim_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-processing-claim-dispositions:7188660939 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-454850853f"></a>
| Concern | Contract |
|---|---|
| <a id="s-c64d0f178c"></a>`application` | riverhog |
| <a id="s-322172bce7"></a>`classification` | client-only-primitive |
| <a id="s-18ec12858e"></a>`cli_commands` | [] |
| <a id="s-d4533d5d1c"></a>`client` | ApiClient |
| <a id="s-6274f827c0"></a>`method` | GET |
| <a id="s-8c4b6e8dd6"></a>`operation_id` | get_processing_claim_dispositions |
| <a id="s-677c6f9e5d"></a>`path` | /v1/collection-processing-claims/{claim_id}/derivation |
| <a id="s-5b88002288"></a>`provider_evidence` | None |
| <a id="s-3d6ec41e8a"></a>`read_collection` | None |
| <a id="s-a16bcdf613"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims/{claim_id}/derivation](../http/get-v1-collection-processing-claims-claim-id-derivation.md)

## Governing policies

- <a id="pa-6112abe728"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-a2d5a6e5e6"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-d8deaef4c1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/31`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4971fdf7ac0c18e64436d2ff2901943a3daf715fe697a4030a24ae0f735b3839 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_processing_claim_dispositions",
  "path": "/v1/collection-processing-claims/{claim_id}/derivation",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
