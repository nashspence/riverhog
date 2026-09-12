# gogurt write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-write:4e97d46046 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt` |
| Interface | `cli` |
| Family | `write` |
| Contract elements | 1 |
| Extent decisions | 7 |

## Machine authority

- `/external_contract/cli/gogurt/commands/write/name`
- `/external_contract/cli/gogurt/commands/write/parameters`

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
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `write`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `route` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | route |
| `mount_point` | TyperArgument | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | mount_point |
| `config` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'path'} | --config |
| `force` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --force |
| `dry_run` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --dry-run |
| `json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |
| `mounted_volume_provider` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --mounted-volume-provider |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/write/name`

<!-- exact-contract-value: bc2434a12fecfa191ab66b56a67f8fe2507216b76be96a72b1f866562cdc3567 -->

```json
"write"
```

### `/external_contract/cli/gogurt/commands/write/parameters`

<!-- exact-contract-value: b3d48e6509916c8278252ee2275d02848c9d2f69f23af502a7b82e22f6097f49 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "route",
    "nargs": 1,
    "options": [
      "route"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
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
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "force",
    "nargs": 1,
    "options": [
      "--force"
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
