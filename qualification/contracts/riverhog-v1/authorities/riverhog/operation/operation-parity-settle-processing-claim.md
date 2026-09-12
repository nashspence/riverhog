# Operation parity: settle_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-settle-processing-claim:4357c41bf5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-244f5384a5"></a>
| Concern | Contract |
|---|---|
| <a id="s-ecb9d7e0ba"></a>`application` | riverhog |
| <a id="s-e270df3a7b"></a>`classification` | client-only-primitive |
| <a id="s-43cdeae3b0"></a>`cli_commands` | [] |
| <a id="s-cfc546710a"></a>`client` | ApiClient |
| <a id="s-8971d8322e"></a>`method` | POST |
| <a id="s-d9e4225f55"></a>`operation_id` | settle_processing_claim |
| <a id="s-b4ef4a40bf"></a>`path` | /v1/collection-processing-claims/{claim_id}/settle |
| <a id="s-15c1979409"></a>`provider_evidence` | None |
| <a id="s-dd8453dafb"></a>`read_collection` | None |
| <a id="s-5c4f47a6ae"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/settle](../http/post-v1-collection-processing-claims-claim-id-settle.md)

## Governing policies

- <a id="pa-65b6efead8"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-0b61da62d4"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-ca640b157b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/50`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b491cdff3c4fe6897597195a57e2d73362d8291fcbc9a4ebff1d86f44bd68bee -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "settle_processing_claim",
  "path": "/v1/collection-processing-claims/{claim_id}/settle",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
