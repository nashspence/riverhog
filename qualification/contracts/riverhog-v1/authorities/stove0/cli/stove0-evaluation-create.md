# stove0 evaluation create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0-evaluation-create:4624c1ec45 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `cli` |
| Family | `evaluation` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/cli/stove0/commands/evaluation/commands/create/name`
- `/external_contract/cli/stove0/commands/evaluation/commands/create/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0` — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: create_evaluation](../operation/operation-parity-create-evaluation.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `create`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `definition` | TyperArgument | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | definition |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/evaluation/commands/create/name`

<!-- exact-contract-value: 5498a731a187f424a5800943afcba027f3a6cd684e38fe6e40c02bee1753152d -->

```json
"create"
```

### `/external_contract/cli/stove0/commands/evaluation/commands/create/parameters`

<!-- exact-contract-value: 4fb330ee8d838285ee3bb2f2f8c99dccc40350eb1951a541aaaddd3d0f750f22 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "definition",
    "nargs": 1,
    "options": [
      "definition"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "path"
    }
  }
]
```
