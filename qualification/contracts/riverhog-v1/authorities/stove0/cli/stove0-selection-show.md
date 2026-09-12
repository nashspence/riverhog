# stove0 selection show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0-selection-show:48819b5e26 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `cli` |
| Family | `selection` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/cli/stove0/commands/selection/commands/show/name`
- `/external_contract/cli/stove0/commands/selection/commands/show/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0` — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: get_artifact_selection](../operation/operation-parity-get-artifact-selection.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `show`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `selection_sha256` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | selection_sha256 |
| `continuation` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --continuation |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/selection/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/stove0/commands/selection/commands/show/parameters`

<!-- exact-contract-value: 0b10d00386773bfc4b66ab848fefb0e9ce0dfa31cd1793be7c11acef3238bc00 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "selection_sha256",
    "nargs": 1,
    "options": [
      "selection_sha256"
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
    "name": "continuation",
    "nargs": 1,
    "options": [
      "--continuation"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  }
]
```
