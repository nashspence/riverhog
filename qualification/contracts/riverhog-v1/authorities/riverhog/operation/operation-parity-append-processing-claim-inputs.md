# Operation parity: append_processing_claim_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-append-processing-claim-inputs:9731d29887 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f7dcf8e602"></a>
| Concern | Contract |
|---|---|
| <a id="s-1ef896961e"></a>`application` | riverhog |
| <a id="s-9eeecbf879"></a>`classification` | client-only-primitive |
| <a id="s-876680325c"></a>`cli_commands` | [] |
| <a id="s-85ddb82f24"></a>`client` | ApiClient |
| <a id="s-099098950b"></a>`method` | PUT |
| <a id="s-9ec061c1e9"></a>`operation_id` | append_processing_claim_inputs |
| <a id="s-41ec12412c"></a>`path` | /v1/collection-processing-claims/{claim_id}/inputs |
| <a id="s-c1ddafa6d8"></a>`provider_evidence` | None |
| <a id="s-8a5b96df6d"></a>`read_collection` | None |
| <a id="s-d83dda8941"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [PUT /v1/collection-processing-claims/{claim_id}/inputs](../http/put-v1-collection-processing-claims-claim-id-inputs.md)

## Governing policies

- <a id="pa-c9d6822f96"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-d68d90fed0"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-5114ed2b4f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/38`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3bf729a9931c39b1cf6357988157d9ffa44f2199680ba9fdb7ba7fc83266a41a -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "PUT",
  "operation_id": "append_processing_claim_inputs",
  "path": "/v1/collection-processing-claims/{claim_id}/inputs",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
