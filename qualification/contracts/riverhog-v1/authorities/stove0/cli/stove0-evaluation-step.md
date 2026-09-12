# stove0 evaluation step

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0-evaluation-step:8127bb41d3 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `cli` |
| Family | `evaluation` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/cli/stove0/commands/evaluation/commands/step/name`
- `/external_contract/cli/stove0/commands/evaluation/commands/step/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0` — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: step_evaluation](../operation/operation-parity-step-evaluation.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `step`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `evaluation_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | evaluation_id |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/evaluation/commands/step/name`

<!-- exact-contract-value: 0d9f50d8178cb7c5b044c4dce43f1a35c44697ac03e70407e2dda2324fa92f56 -->

```json
"step"
```

### `/external_contract/cli/stove0/commands/evaluation/commands/step/parameters`

<!-- exact-contract-value: ca673eb21049942d705991d78449bdc62878a1a010aacb714cce5fe46aff8f88 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "evaluation_id",
    "nargs": 1,
    "options": [
      "evaluation_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  }
]
```
