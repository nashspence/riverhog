# gogurt run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-run:f4364383c7 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt` |
| Interface | `cli` |
| Family | `run` |
| Contract elements | 1 |
| Extent decisions | 6 |

## Machine authority

- `/external_contract/cli/gogurt/commands/run/name`
- `/external_contract/cli/gogurt/commands/run/parameters`

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
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `run`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `mount_point` | TyperArgument | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | mount_point |
| `config` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'path'} | --config |
| `actions_dir` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'path'} | --actions-dir |
| `autorun` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --autorun |
| `dry_run` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --dry-run |
| `mounted_volume_provider` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --mounted-volume-provider |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/run/name`

<!-- exact-contract-value: 5e87f618bd8837e87070ae7f83753c6a23ff095f43de6ededcd38ae535031c29 -->

```json
"run"
```

### `/external_contract/cli/gogurt/commands/run/parameters`

<!-- exact-contract-value: 71334cbe42b63de19c542866177a9d9043a11b263fe1bbf92ac6bfe356af284f -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "mount_point",
    "nargs": 1,
    "options": [
      "mount_point"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "path"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "config",
    "nargs": 1,
    "options": [
      "--config"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "path"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "actions_dir",
    "nargs": 1,
    "options": [
      "--actions-dir"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "path"
    }
  },
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "autorun",
    "nargs": 1,
    "options": [
      "--autorun"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  },
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "dry_run",
    "nargs": 1,
    "options": [
      "--dry-run"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  },
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
  }
]
```
