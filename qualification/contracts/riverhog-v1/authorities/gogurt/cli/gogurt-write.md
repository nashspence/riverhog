# gogurt write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-write:4e97d46046 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [cli](index.md) |
| Family | [write](index.md#f-b46409492b) |
| Contract elements | 1 |
| Extent decisions | 7 |

## External contract

- <a id="s-8595e8e5ab"></a>Parser name: `write`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-e3296e8a22"></a>`route` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | route |
| <a id="s-08581ea9b7"></a>`mount_point` | TyperArgument | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | mount_point |
| <a id="s-f0b9492d8e"></a>`config` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'path'} | --config |
| <a id="s-aba7dbf38e"></a>`force` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --force |
| <a id="s-cef35a84fe"></a>`dry_run` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --dry-run |
| <a id="s-d2fbb045b4"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |
| <a id="s-5d3f067691"></a>`mounted_volume_provider` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --mounted-volume-provider |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --config](#s-f0b9492d8e) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --dry-run](#s-cef35a84fe) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --force](#s-aba7dbf38e) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-d2fbb045b4) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter mount_point](#s-08581ea9b7) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --mounted-volume-provider](#s-5d3f067691) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter route](#s-e3296e8a22) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-b6ee7f15d7"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-75c18e8e99"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/write/name`
- `/external_contract/cli/gogurt/commands/write/parameters`

### Exact owned JSON

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
