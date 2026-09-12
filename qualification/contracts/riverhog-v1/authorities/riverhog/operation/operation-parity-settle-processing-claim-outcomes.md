# Operation parity: settle_processing_claim_outcomes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-settle-processing-claim-outcomes:32b0539d7d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c63fbf1472"></a>
| Concern | Contract |
|---|---|
| <a id="s-706272e402"></a>`application` | riverhog |
| <a id="s-0a537cf5ea"></a>`classification` | client-only-primitive |
| <a id="s-a4c9089792"></a>`cli_commands` | [] |
| <a id="s-99d1fddf34"></a>`client` | ApiClient |
| <a id="s-22900a9ef5"></a>`method` | POST |
| <a id="s-cafbb78e42"></a>`operation_id` | settle_processing_claim_outcomes |
| <a id="s-256b79e82f"></a>`path` | /v1/collection-processing-claims/{claim_id}/outcomes/settle |
| <a id="s-3742ee1a84"></a>`provider_evidence` | None |
| <a id="s-4cf300b89c"></a>`read_collection` | None |
| <a id="s-3827f27e92"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/outcomes/settle](../http/post-v1-collection-processing-claims-claim-id-outcomes-settle.md)

## Governing policies

- <a id="pa-42dea913fd"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-ca009b377f"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-811bdf4c54"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/41`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c91791ea5f9789489cf8e56ba520e8118446b5f856eeda44a80b799670d51c39 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "settle_processing_claim_outcomes",
  "path": "/v1/collection-processing-claims/{claim_id}/outcomes/settle",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
