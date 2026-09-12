# Operation parity: list_processing_claim_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-processing-claim-inputs:5077e11f4c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ae6664c9ba2a"></a>
| Concern | Contract |
|---|---|
| <a id="s-86b55ed613bf"></a>`application` | riverhog |
| <a id="s-a32144bd6cd6"></a>`classification` | client-only-primitive |
| <a id="s-4a6d45d7aad5"></a>`cli_commands` | [] |
| <a id="s-d6c842c2f59c"></a>`client` | ApiClient |
| <a id="s-9ec3304fd3c8"></a>`method` | GET |
| <a id="s-28bf73af0fc5"></a>`operation_id` | list_processing_claim_inputs |
| <a id="s-0508b86d7fb8"></a>`path` | /v1/collection-processing-claims/{claim_id}/inputs |
| <a id="s-03a3f68f5dac"></a>`provider_evidence` | None |
| <a id="s-555a98c35a3a"></a>`read_collection` | {"authority": "processing-claim-inputs", "authority_parameter": "authority_sha256", "cursor_parameter": "start_ordinal", "fixed_limit": 128, "kind": "exact-authority-page"} |
| <a id="s-d5225dda3e3f"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims/{claim_id}/inputs](../http/get-v1-collection-processing-claims-claim-id-inputs.md)

## Governing policies

- <a id="pa-8bb5de73242a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-0083cba94afe"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-8b706affeaae"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/37`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82e5c4207be5425f305da39a6aafab096e1235509747f8e8523bd0e0e9e943ac -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_processing_claim_inputs",
  "path": "/v1/collection-processing-claims/{claim_id}/inputs",
  "provider_evidence": null,
  "read_collection": {
    "authority": "processing-claim-inputs",
    "authority_parameter": "authority_sha256",
    "cursor_parameter": "start_ordinal",
    "fixed_limit": 128,
    "kind": "exact-authority-page"
  },
  "response_authority": "canonical-document"
}
```
