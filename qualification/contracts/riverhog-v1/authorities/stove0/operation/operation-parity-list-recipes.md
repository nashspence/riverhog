# Operation parity: list_recipes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-list-recipes:81d58c55c1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [recipes](families/recipes/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-11db75a3138a"></a>
| Concern | Contract |
|---|---|
| <a id="s-8398ec6fca3e"></a>`application` | stove0 |
| <a id="s-854ef65425f9"></a>`classification` | human-cli+json |
| <a id="s-d3ee5c4b14d8"></a>`cli_commands` | ["recipe list"] |
| <a id="s-b3c83d9efa78"></a>`client` | Stove0ApiClient |
| <a id="s-f07a24f2c7f5"></a>`method` | GET |
| <a id="s-32e45ca65312"></a>`operation_id` | list_recipes |
| <a id="s-78fedb7adb16"></a>`path` | /v1/recipes |
| <a id="s-78d1b63999f5"></a>`provider_evidence` | None |
| <a id="s-6851837462dd"></a>`read_collection` | None |
| <a id="s-665540489d72"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/recipes](../http/get-v1-recipes.md)
- [stove0 recipe list](../cli/stove0-recipe-list.md)

## Governing policies

- <a id="pa-adc23977b5fb"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-2a89e30668ed"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-caad3cd7212b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/132`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7ab96d45cc393e9a881c87a05acb37b0879874d161902b0dfe1699805502b63d -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "recipe list"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "list_recipes",
  "path": "/v1/recipes",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
