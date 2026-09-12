# gogurt listener _run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-run:fc612b6a35 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt` |
| Interface | `cli` |
| Family | `listener` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/_run/name`
- `/external_contract/cli/gogurt/commands/listener/commands/_run/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:gogurt` — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `_run`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `runtime_config` | TyperOption | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | --runtime-config |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/listener/commands/_run/name`

<!-- exact-contract-value: 5f0b1cc0fff7e256dbce362a64c00555715bcd2eb6cd0040718cee353633a7ff -->

```json
"_run"
```

### `/external_contract/cli/gogurt/commands/listener/commands/_run/parameters`

<!-- exact-contract-value: bcc4ae3bf32fb126fe4759e5f09ca3b2a76c909da7f41cd96b017dac9a244080 -->

```json
[
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "runtime_config",
    "nargs": 1,
    "options": [
      "--runtime-config"
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
