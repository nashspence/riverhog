# Operation parity: renew_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-renew-processing-claim:29123aa175 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b3a9dd34b1"></a>
| Concern | Contract |
|---|---|
| <a id="s-ca30c65d37"></a>`application` | riverhog |
| <a id="s-ad324ff89e"></a>`classification` | client-only-primitive |
| <a id="s-e223fd6842"></a>`cli_commands` | [] |
| <a id="s-fbbda48558"></a>`client` | ApiClient |
| <a id="s-9ef0811bb5"></a>`method` | POST |
| <a id="s-f6b2565390"></a>`operation_id` | renew_processing_claim |
| <a id="s-aaeb683424"></a>`path` | /v1/collection-processing-claims/{claim_id}/renew |
| <a id="s-f55552bfda"></a>`provider_evidence` | None |
| <a id="s-39f869d5e9"></a>`read_collection` | None |
| <a id="s-3188fa267e"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/renew](../http/post-v1-collection-processing-claims-claim-id-renew.md)

## Governing policies

- <a id="pa-bce657329b"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-5a7a3aa50d"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-8acdc1e50f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/47`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98e85d4e60b58e93cd4e591090cdccad965c336e531d1050c28d249a9698bdb6 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "renew_processing_claim",
  "path": "/v1/collection-processing-claims/{claim_id}/renew",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
