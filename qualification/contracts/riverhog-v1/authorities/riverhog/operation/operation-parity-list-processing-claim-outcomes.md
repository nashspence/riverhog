# Operation parity: list_processing_claim_outcomes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-processing-claim-outcomes:234ed0ed0d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-2ffa61f47d"></a>
| Concern | Contract |
|---|---|
| <a id="s-4890fe46a0"></a>`application` | riverhog |
| <a id="s-f957175926"></a>`classification` | client-only-primitive |
| <a id="s-46e651fad1"></a>`cli_commands` | [] |
| <a id="s-b387761959"></a>`client` | ApiClient |
| <a id="s-7e5955bc4e"></a>`method` | GET |
| <a id="s-54e0f55617"></a>`operation_id` | list_processing_claim_outcomes |
| <a id="s-8dc519d111"></a>`path` | /v1/collection-processing-claims/{claim_id}/outcomes |
| <a id="s-b9ae16df6b"></a>`provider_evidence` | None |
| <a id="s-8db4995732"></a>`read_collection` | {"authority": "processing-claim-outcomes", "authority_parameter": "authority_sha256", "cursor_parameter": "start_ordinal", "fixed_limit": 128, "kind": "exact-authority-page"} |
| <a id="s-6c60ae4850"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims/{claim_id}/outcomes](../http/get-v1-collection-processing-claims-claim-id-outcomes.md)

## Governing policies

- <a id="pa-8fa4e717ec"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-bdaf2e2fc7"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-381d6aca65"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/40`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: efc7ce19dad641fd2edda90cb4ce84c82301135f853c500a6ee825280c573c3e -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_processing_claim_outcomes",
  "path": "/v1/collection-processing-claims/{claim_id}/outcomes",
  "provider_evidence": null,
  "read_collection": {
    "authority": "processing-claim-outcomes",
    "authority_parameter": "authority_sha256",
    "cursor_parameter": "start_ordinal",
    "fixed_limit": 128,
    "kind": "exact-authority-page"
  },
  "response_authority": "canonical-document"
}
```
