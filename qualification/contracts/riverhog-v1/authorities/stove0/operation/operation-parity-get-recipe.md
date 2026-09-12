# Operation parity: get_recipe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-get-recipe:ee15680607 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `recipes` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/133`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/recipes/{recipe_id}](../http/get-v1-recipes-recipe-id.md)
- [stove0 recipe show](../cli/stove0-recipe-show.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["recipe show"] |
| `client` | Stove0ApiClient |
| `method` | GET |
| `operation_id` | get_recipe |
| `path` | /v1/recipes/{recipe_id} |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |
