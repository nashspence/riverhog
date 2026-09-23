# gogurt run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-run:5b0e244001 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-aba9c0e5e0"></a>Parser name: `run`
- <a id="s-1ac5f267a8"></a>Extra arguments at this parser: rejected.
- <a id="s-d87ab3647e"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-0fbd550c70"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-8087dcff61"></a>`mount_point`<br>`mount_point` | required positional; 1 value | path; existence not required; regular files allowed; directories allowed; access checks on existing paths: read; resolve absolute path and symlinks: no; dash uses normal path checks | not recorded<br>Env: `null` |
| <a id="s-6f8a90a5e2"></a>`config`<br>`--config` | optional option; 1 value | path; existence not required; regular files allowed; directories allowed; access checks on existing paths: read; resolve absolute path and symlinks: no; dash uses normal path checks | not recorded<br>Env: `null` |
| <a id="s-dad97bb92b"></a>`actions_dir`<br>`--actions-dir` | optional option; 1 value | path; existence not required; regular files allowed; directories allowed; access checks on existing paths: read; resolve absolute path and symlinks: no; dash uses normal path checks | not recorded<br>Env: `null` |
| <a id="s-8cb427b8d5"></a>`autorun`<br>`--autorun` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-1e601a6dd2"></a>`dry_run`<br>`--dry-run` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-2c39914b8b"></a>`mounted_volume_provider`<br>`--mounted-volume-provider` | optional option; 1 value | text | not recorded<br>Env: `"GOGURT_MOUNTED_VOLUME_PROVIDER"` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-2ac14836a3"></a>`help` | <a id="s-68f5a1449f"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-7f93c04341"></a>`0` | <a id="s-502c152b96"></a>`"noncontractual-framework-help"` | <a id="s-963fadb4fd"></a>`"empty"` |

### Result and failure contract

- <a id="s-2790d5673a"></a>Result identity: `gogurt-cli-result/run/v1`
- <a id="s-4b07394551"></a>Profile: `gogurt-cli-action/v1`
- <a id="s-615efaeeab"></a>Structured output: `none`
- <a id="s-e0e77312b3"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-df7255116d"></a>`completed-or-not-run` | <a id="s-171e12c5c1"></a>`{"kind":"action-returned-zero"}` | <a id="s-5accadf616"></a>`0` | <a id="s-4e18a34210"></a>human: `"empty"` | <a id="s-55d1e1187d"></a>human: `"noncontractual-action-status-or-empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-571ddc7f7f"></a>`usage` | <a id="s-2d7cf89db4"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-438cffd1ea"></a>`2` | <a id="s-464bf046d5"></a>human: `"empty"` | <a id="s-d0a103d44d"></a>human: `"noncontractual-usage-diagnostic"` |
| <a id="s-9c5aba5f76"></a>`operational` | <a id="s-af9f4b40e8"></a>`{"kind":"application-error"}` | <a id="s-3ac370c105"></a>`1` | <a id="s-62c1cb47a5"></a>human: `"empty"` | <a id="s-19e1ff480b"></a>human: `"noncontractual-diagnostic"` |
| <a id="s-e52e46f45e"></a>`action-exit` | <a id="s-01f724acdf"></a>`{"kind":"delegated-action-returned-nonzero"}` | <a id="s-3601b6f0ed"></a>`{"kind":"delegated","maximum":255,"minimum":1}` | <a id="s-a0c4ce7255"></a>human: `"child-process-owned"` | <a id="s-d08c31c5f8"></a>human: `"child-process-owned-and-action-status"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --actions-dir](#s-dad97bb92b) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --autorun](#s-8cb427b8d5) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --config](#s-6f8a90a5e2) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --dry-run](#s-1e601a6dd2) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter mount_point](#s-8087dcff61) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --mounted-volume-provider](#s-2c39914b8b) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-0f66efd540"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-47f10516dc"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources/authorities.md#src-3b2297c37d) — [reference/gogurt/application/src/gogurt/cli.py::&lt;module&gt;](../../../../../../reference/gogurt/application/src/gogurt/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/gogurt/commands/run/allow_extra_args`
- `/external_contract/cli/gogurt/commands/run/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/run/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/run/name`
- `/external_contract/cli/gogurt/commands/run/parameters`
- `/external_contract/cli/gogurt/commands/run/result_contract`
- `/external_contract/cli/gogurt/commands/run/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/run/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/run/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/run/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/run/name`

<!-- exact-contract-value: 5e87f618bd8837e87070ae7f83753c6a23ff095f43de6ededcd38ae535031c29 -->

```json
"run"
```

### `/external_contract/cli/gogurt/commands/run/parameters`

<!-- exact-contract-value: e3ca5466064a3767e2faa1a50274c4cc04a876c72ef0e39c91c7eafbba7b3392 -->

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
      "allow_dash": false,
      "class": "typer.models.TyperPath",
      "dir_okay": true,
      "exists": false,
      "file_okay": true,
      "name": "path",
      "readable": true,
      "resolve_path": false,
      "writable": false
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
      "allow_dash": false,
      "class": "typer.models.TyperPath",
      "dir_okay": true,
      "exists": false,
      "file_okay": true,
      "name": "path",
      "readable": true,
      "resolve_path": false,
      "writable": false
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
      "allow_dash": false,
      "class": "typer.models.TyperPath",
      "dir_okay": true,
      "exists": false,
      "file_okay": true,
      "name": "path",
      "readable": true,
      "resolve_path": false,
      "writable": false
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

<!-- exact-contract-value: f3c55fa703fcc528445b10ee6e7e785569ee31508516568ba1cc02d25978913f -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
      "selected_by": {
        "kind": "parser-rejected-invocation"
      },
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
      "selected_by": {
        "kind": "application-error"
      },
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
      "selected_by": {
        "kind": "delegated-action-returned-nonzero"
      },
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
      "selected_by": {
        "kind": "action-returned-zero"
      },
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

### `/external_contract/cli/gogurt/commands/run/terminating_controls`

<!-- exact-contract-value: 654ffd6937a42b17b4204e0750fd74bd42751d2241f4a4632015efb38811a79c -->

```json
[
  {
    "exit_status": 0,
    "id": "help",
    "stderr": "empty",
    "stdout": "noncontractual-framework-help",
    "trigger": {
      "kind": "option-present",
      "options": [
        "--help"
      ]
    }
  }
]
```

</details>
