# Operation parity: get_collection_derivation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-collection-derivation:98ed3a41a6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1d94f1f636"></a>
| Concern | Contract |
|---|---|
| <a id="s-888af324be"></a>`application` | riverhog |
| <a id="s-89b032e823"></a>`classification` | client-only-primitive |
| <a id="s-634fa09bd6"></a>`cli_commands` | [] |
| <a id="s-fa0bbc22d0"></a>`client` | ApiClient |
| <a id="s-c337f84329"></a>`method` | GET |
| <a id="s-68da2f7a4c"></a>`operation_id` | get_collection_derivation |
| <a id="s-ed198f9a09"></a>`path` | /v1/collections/{collection_id}/derivation |
| <a id="s-8c091b334f"></a>`provider_evidence` | None |
| <a id="s-eb41013e41"></a>`read_collection` | None |
| <a id="s-3d90fd5536"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/derivation](../http/get-v1-collections-collection-id-derivation.md)

## Governing policies

- <a id="pa-e8540cd55c"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-083367cdbf"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-8002613a60"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/75`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: df97d5d505b4afa9c93b39dfc94651a2fef2731b72447dd7a5c1caa5d9a21645 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_collection_derivation",
  "path": "/v1/collections/{collection_id}/derivation",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
