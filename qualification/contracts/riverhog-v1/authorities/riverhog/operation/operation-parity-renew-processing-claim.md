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

<a id="s-b3a9dd34b1c7"></a>
| Concern | Contract |
|---|---|
| <a id="s-ca30c65d374e"></a>`application` | riverhog |
| <a id="s-ad324ff89ef6"></a>`classification` | client-only-primitive |
| <a id="s-e223fd684288"></a>`cli_commands` | [] |
| <a id="s-fbbda48558b6"></a>`client` | ApiClient |
| <a id="s-9ef0811bb526"></a>`method` | POST |
| <a id="s-f6b2565390bf"></a>`operation_id` | renew_processing_claim |
| <a id="s-aaeb6834249f"></a>`path` | /v1/collection-processing-claims/{claim_id}/renew |
| <a id="s-f55552bfda18"></a>`provider_evidence` | None |
| <a id="s-39f869d5e96a"></a>`read_collection` | None |
| <a id="s-3188fa267e61"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/renew](../http/post-v1-collection-processing-claims-claim-id-renew.md)

## Governing policies

- <a id="pa-bce657329b8a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-5a7a3aa50d56"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-8acdc1e50fe6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

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
