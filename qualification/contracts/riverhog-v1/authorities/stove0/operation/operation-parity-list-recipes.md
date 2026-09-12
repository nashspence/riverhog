# Operation parity: list_recipes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-list-recipes:81d58c55c1 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `recipes` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/132`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/recipes](../http/get-v1-recipes.md)
- [stove0 recipe list](../cli/stove0-recipe-list.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["recipe list"] |
| `client` | Stove0ApiClient |
| `method` | GET |
| `operation_id` | list_recipes |
| `path` | /v1/recipes |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |

## Complete owned contract

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
