# stove0 recipe show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0-recipe-show:28f4e10e60 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [cli](index.md) |
| Family | [recipe](families/recipe/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- <a id="s-116f2b9e63"></a>Parser name: `show`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-66aae1f8d7"></a>`recipe_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | recipe_id |
| <a id="s-cda51628b9"></a>`revision` | TyperOption | no | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | --revision |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter recipe_id](#s-66aae1f8d7) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --revision](#s-cda51628b9) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: get_recipe](../operation/operation-parity-get-recipe.md)

## Governing policies

- <a id="pa-f3274c1979"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-53c7ad3d31"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/recipe/commands/show/name`
- `/external_contract/cli/stove0/commands/recipe/commands/show/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/recipe/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/stove0/commands/recipe/commands/show/parameters`

<!-- exact-contract-value: b1ae0d39b9c52a02c16ef66a0728c68179d0e233305ec5e56c7fc2d9f0dfe8b5 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "recipe_id",
    "nargs": 1,
    "options": [
      "recipe_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "revision",
    "nargs": 1,
    "options": [
      "--revision"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntParamType",
      "name": "integer"
    }
  }
]
```
