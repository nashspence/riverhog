# Operation parity: abandon_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-abandon-processing-claim:008f825ed3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-968a80937d"></a>
| Concern | Contract |
|---|---|
| <a id="s-24e9bfaf9e"></a>`application` | riverhog |
| <a id="s-feff28ae5e"></a>`classification` | client-only-primitive |
| <a id="s-66f51a8fa0"></a>`cli_commands` | [] |
| <a id="s-635f50e85d"></a>`client` | ApiClient |
| <a id="s-79a391d3f8"></a>`method` | POST |
| <a id="s-b932a5c10f"></a>`operation_id` | abandon_processing_claim |
| <a id="s-db83382962"></a>`path` | /v1/collection-processing-claims/{claim_id}/abandon |
| <a id="s-00f46f7357"></a>`provider_evidence` | None |
| <a id="s-b3a51b30a1"></a>`read_collection` | None |
| <a id="s-f5afda7695"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/abandon](../http/post-v1-collection-processing-claims-claim-id-abandon.md)

## Governing policies

- <a id="pa-02ec3839c4"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c7d8fde11e"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-faf899cb29"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/27`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fffb5468d6f227524aa437e1917af1750a0c30320d4bf6a937cd3ecfa7a24400 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "abandon_processing_claim",
  "path": "/v1/collection-processing-claims/{claim_id}/abandon",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
