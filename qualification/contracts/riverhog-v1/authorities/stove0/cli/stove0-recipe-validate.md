# stove0 recipe validate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0-recipe-validate:81f7e836dc -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `cli` |
| Family | `recipe` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/cli/stove0/commands/recipe/commands/validate/name`
- `/external_contract/cli/stove0/commands/recipe/commands/validate/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0` — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `validate`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `path` | TyperArgument | yes | {'class': 'typer.models.TyperPath', 'name': 'file'} | path |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/recipe/commands/validate/name`

<!-- exact-contract-value: 2c9877104bf173f3ecf6b4d44be3e77da3b1b3c87e448be5f6cec01b12ccf804 -->

```json
"validate"
```

### `/external_contract/cli/stove0/commands/recipe/commands/validate/parameters`

<!-- exact-contract-value: 16bb2841bd48031955d2caf741ae9c4ff040273016852cfe532eb80ee09dbe8b -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "path",
    "nargs": 1,
    "options": [
      "path"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "file"
    }
  }
]
```
