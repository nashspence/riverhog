# gogurt run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-run:033025c3bc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-aba9c0e5e0"></a>Parser name: `run`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-8087dcff61"></a>`mount_point` | TyperArgument | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | mount_point |
| <a id="s-6f8a90a5e2"></a>`config` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'path'} | --config |
| <a id="s-dad97bb92b"></a>`actions_dir` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'path'} | --actions-dir |
| <a id="s-8cb427b8d5"></a>`autorun` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --autorun |
| <a id="s-1e601a6dd2"></a>`dry_run` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --dry-run |
| <a id="s-2c39914b8b"></a>`mounted_volume_provider` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --mounted-volume-provider |

### Result and failure contract

- <a id="s-2790d5673a"></a>Result identity: `gogurt-cli-result/run/v1`
- <a id="s-4b07394551"></a>Profile: `gogurt-cli-action/v1`
- <a id="s-615efaeeab"></a>Structured output: `none`
- <a id="s-e0e77312b3"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-df7255116d"></a>`completed-or-not-run` | <a id="s-5accadf616"></a>`0` | <a id="s-4e18a34210"></a>`{"human":"empty"}` | <a id="s-55d1e1187d"></a>`{"human":"noncontractual-action-status-or-empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-571ddc7f7f"></a>`usage` | <a id="s-438cffd1ea"></a>`2` | <a id="s-464bf046d5"></a>`{"human":"empty"}` | <a id="s-d0a103d44d"></a>`{"human":"noncontractual-usage-diagnostic"}` |
| <a id="s-9c5aba5f76"></a>`operational` | <a id="s-3ac370c105"></a>`1` | <a id="s-62c1cb47a5"></a>`{"human":"empty"}` | <a id="s-19e1ff480b"></a>`{"human":"noncontractual-diagnostic"}` |
| <a id="s-e52e46f45e"></a>`action-exit` | <a id="s-3601b6f0ed"></a>`{"kind":"delegated","maximum":255,"minimum":1}` | <a id="s-a0c4ce7255"></a>`{"human":"child-process-owned"}` | <a id="s-d08c31c5f8"></a>`{"human":"child-process-owned-and-action-status"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --actions-dir](#s-dad97bb92b) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --autorun](#s-8cb427b8d5) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --config](#s-6f8a90a5e2) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --dry-run](#s-1e601a6dd2) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter mount_point](#s-8087dcff61) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --mounted-volume-provider](#s-2c39914b8b) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-8877b2009c"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-679a7b1142"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/run/name`
- `/external_contract/cli/gogurt/commands/run/parameters`
- `/external_contract/cli/gogurt/commands/run/result_contract`

### Exact owned JSON

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

### `/external_contract/cli/gogurt/commands/run/result_contract`

<!-- exact-contract-value: ef86772f4e4691ec0c4697714efdede888d7c99a35c059b81f75d2e53fb93a18 -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
      "stderr": {
        "human": "noncontractual-usage-diagnostic"
      },
      "stdout": {
        "human": "empty"
      }
    },
    {
      "exit_status": 1,
      "id": "operational",
      "stderr": {
        "human": "noncontractual-diagnostic"
      },
      "stdout": {
        "human": "empty"
      }
    },
    {
      "exit_status": {
        "kind": "delegated",
        "maximum": 255,
        "minimum": 1
      },
      "id": "action-exit",
      "stderr": {
        "human": "child-process-owned-and-action-status"
      },
      "stdout": {
        "human": "child-process-owned"
      }
    }
  ],
  "human_json_relationship": "not-applicable",
  "identity": "gogurt-cli-result/run/v1",
  "profile_id": "gogurt-cli-action/v1",
  "structured_output": "none",
  "success": [
    {
      "exit_status": 0,
      "id": "completed-or-not-run",
      "stderr": {
        "human": "noncontractual-action-status-or-empty"
      },
      "stdout": {
        "human": "empty"
      }
    }
  ]
}
```
