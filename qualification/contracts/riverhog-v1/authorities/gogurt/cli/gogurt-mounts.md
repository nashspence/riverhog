# gogurt mounts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-mounts:05f6dee923 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt` |
| Interface | `cli` |
| Family | `mounts` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/cli/gogurt/commands/mounts/name`
- `/external_contract/cli/gogurt/commands/mounts/parameters`

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
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `mounts`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `mounted_volume_provider` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --mounted-volume-provider |
| `json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/mounts/name`

<!-- exact-contract-value: 09a1a78edad0483f330a615c7189ec42febdd6dee239f4f50c1c1809a5cb52a3 -->

```json
"mounts"
```

### `/external_contract/cli/gogurt/commands/mounts/parameters`

<!-- exact-contract-value: 83c1089fc312d926e38bc2c1895b5a74fe68544af45e9bfb4d7c6279a94e8215 -->

```json
[
  {
    "count": false,
    "envvar": "GOGURT_MOUNTED_VOLUME_PROVIDER",
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "mounted_volume_provider",
    "nargs": 1,
    "options": [
      "--mounted-volume-provider"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "json_mode",
    "nargs": 1,
    "options": [
      "--json"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  }
]
```
