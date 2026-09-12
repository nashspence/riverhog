# Operation parity: get_recipe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-get-recipe:ee15680607 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [recipes](families/recipes/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e3dc021f45ea"></a>
| Concern | Contract |
|---|---|
| <a id="s-9f8518a32204"></a>`application` | stove0 |
| <a id="s-b51e2164b06b"></a>`classification` | human-cli+json |
| <a id="s-f3f21d439424"></a>`cli_commands` | ["recipe show"] |
| <a id="s-f76fd2ec3176"></a>`client` | Stove0ApiClient |
| <a id="s-aba7a9cd9073"></a>`method` | GET |
| <a id="s-4ab88d8ee7dd"></a>`operation_id` | get_recipe |
| <a id="s-f9c363ce778d"></a>`path` | /v1/recipes/{recipe_id} |
| <a id="s-72d9e1057261"></a>`provider_evidence` | None |
| <a id="s-7d717d49f888"></a>`read_collection` | None |
| <a id="s-a094098833bd"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/recipes/{recipe_id}](../http/get-v1-recipes-recipe-id.md)
- [stove0 recipe show](../cli/stove0-recipe-show.md)

## Governing policies

- <a id="pa-91acc4e9bdcd"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-21fe4161db94"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-1605458f83d0"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/133`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 056806de4682ba2122393ac1620d561c9efc8f9f520303afefdd324dffaf72b5 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "recipe show"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "get_recipe",
  "path": "/v1/recipes/{recipe_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
