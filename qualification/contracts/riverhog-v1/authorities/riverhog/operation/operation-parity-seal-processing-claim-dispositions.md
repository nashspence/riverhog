# Operation parity: seal_processing_claim_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-seal-processing-claim-dispositions:4d5a928287 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-dba0238fc2a9"></a>
| Concern | Contract |
|---|---|
| <a id="s-13f02212de4d"></a>`application` | riverhog |
| <a id="s-8ce8c2d647fe"></a>`classification` | client-only-primitive |
| <a id="s-db5bfbce90e4"></a>`cli_commands` | [] |
| <a id="s-ca61a2b5ab8d"></a>`client` | ApiClient |
| <a id="s-982776a6f07f"></a>`method` | POST |
| <a id="s-5e1fec5077fd"></a>`operation_id` | seal_processing_claim_dispositions |
| <a id="s-b61a11763258"></a>`path` | /v1/collection-processing-claims/{claim_id}/derivation/seal |
| <a id="s-87d43d4b7988"></a>`provider_evidence` | None |
| <a id="s-03339156d269"></a>`read_collection` | None |
| <a id="s-7b1a13ca6733"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/derivation/seal](../http/post-v1-collection-processing-claims-claim-id-derivation-seal.md)

## Governing policies

- <a id="pa-2432238f1f45"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-6b569e3b111b"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-6f2878cbf4a6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/36`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 80a7ed04f169d8c180276638706daf400a412e1ba509e32a351edaf6ba3d4307 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "seal_processing_claim_dispositions",
  "path": "/v1/collection-processing-claims/{claim_id}/derivation/seal",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
