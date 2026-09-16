# gogurt watch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-watch:00fb8e17c9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d36da8c1e2"></a>Parser name: `watch`
- <a id="s-58e5c19564"></a>Extra arguments at this parser: rejected.
- <a id="s-cff163cdcb"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-2e11b1d8b5"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-8c1c3a78df"></a>`config`<br>`--config` | optional option; 1 value | path; existence not required; regular files allowed; directories allowed; access checks on existing paths: read; resolve absolute path and symlinks: no; dash uses normal path checks | not recorded |
| <a id="s-e1a415005b"></a>`actions_dir`<br>`--actions-dir` | optional option; 1 value | path; existence not required; regular files allowed; directories allowed; access checks on existing paths: read; resolve absolute path and symlinks: no; dash uses normal path checks | not recorded |
| <a id="s-50b8960c86"></a>`interval_seconds`<br>`--interval` | optional option; 1 value | float range; minimum=`0.1` (inclusive); maximum=`3600` (inclusive); outside range: reject | `2` |
| <a id="s-de6a5f63a0"></a>`include_existing`<br>`--include-existing` | optional flag; 0 values | boolean | `false` |
| <a id="s-efe1748062"></a>`autorun`<br>`--autorun` | optional flag; 0 values | boolean | `false` |
| <a id="s-f5f158bdd9"></a>`dry_run`<br>`--dry-run` | optional flag; 0 values | boolean | `false` |
| <a id="s-b1aab2a4f4"></a>`mounted_volume_provider`<br>`--mounted-volume-provider` | optional option; 1 value | text | not recorded<br>Env: `"GOGURT_MOUNTED_VOLUME_PROVIDER"` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-697a89a8bc"></a>`help` | <a id="s-96fe1be67f"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-0e07d55d5b"></a>`0` | <a id="s-fd17924e1c"></a>`"noncontractual-framework-help"` | <a id="s-175b0299d9"></a>`"empty"` |

### Result and failure contract

- <a id="s-6df9422bdb"></a>Result identity: `gogurt-cli-result/watch/v1`
- <a id="s-5f0738a02f"></a>Profile: `gogurt-cli-listener-runtime/v1`
- <a id="s-263ee9aec3"></a>Structured output: `none`
- <a id="s-b1bdec60c8"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-08b0b33d5a"></a>`stopped` | <a id="s-fe64d7000f"></a>`{"kind":"listener-runtime-returned"}` | <a id="s-65096d6a0a"></a>`0` | <a id="s-71850a738c"></a>human: `no-command-result` | <a id="s-98ee100534"></a>human: `noncontractual-runtime-status` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-34f0427187"></a>`usage` | <a id="s-7c42ea8a83"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-745561a71b"></a>`2` | <a id="s-e2ff4ffd41"></a>human: `empty` | <a id="s-197b5b56cc"></a>human: `noncontractual-usage-diagnostic` |
| <a id="s-094ef85d1b"></a>`operational` | <a id="s-76c82c7c68"></a>`{"kind":"application-error"}` | <a id="s-ccc9c5351d"></a>`1` | <a id="s-504d0d2d13"></a>human: `empty` | <a id="s-369d48cc02"></a>human: `noncontractual-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --actions-dir](#s-e1a415005b) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --autorun](#s-efe1748062) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --config](#s-8c1c3a78df) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --dry-run](#s-f5f158bdd9) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --include-existing](#s-de6a5f63a0) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --interval](#s-50b8960c86) | `value · cli-value · contract_max` | maximum=3600; minimum=0.1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --interval](#s-50b8960c86) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --mounted-volume-provider](#s-b1aab2a4f4) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Governing policies

- <a id="pa-98faceebfa"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-17d44e24ac"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/watch/allow_extra_args`
- `/external_contract/cli/gogurt/commands/watch/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/watch/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/watch/name`
- `/external_contract/cli/gogurt/commands/watch/parameters`
- `/external_contract/cli/gogurt/commands/watch/result_contract`
- `/external_contract/cli/gogurt/commands/watch/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/watch/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/watch/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/watch/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/watch/name`

<!-- exact-contract-value: 73da76bff71a604995ddd94e223ffa8b7c171b54e0a953c0fb794ac85a61534b -->

```json
"watch"
```

### `/external_contract/cli/gogurt/commands/watch/parameters`

<!-- exact-contract-value: 0cb5b8772c1e16cf43f6f35118e0eaa4c7f55536d7e7eafab74db95bcb9a4e6e -->

```json
[
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
    "default": 2,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "interval_seconds",
    "nargs": 1,
    "options": [
      "--interval"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "clamp": false,
      "class": "typer._click.types.FloatRange",
      "max_open": false,
      "maximum": 3600,
      "min_open": false,
      "minimum": 0.1,
      "name": "float range"
    }
  },
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "include_existing",
    "nargs": 1,
    "options": [
      "--include-existing"
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

### `/external_contract/cli/gogurt/commands/watch/result_contract`

<!-- exact-contract-value: 16141b88f4eb953926f795d310f341b4d6ab3549c13994817cb1b88555037234 -->

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
    }
  ],
  "human_json_relationship": "not-applicable",
  "identity": "gogurt-cli-result/watch/v1",
  "profile_id": "gogurt-cli-listener-runtime/v1",
  "structured_output": "none",
  "success": [
    {
      "exit_status": 0,
      "id": "stopped",
      "selected_by": {
        "kind": "listener-runtime-returned"
      },
      "stderr": {
        "human": "noncontractual-runtime-status"
      },
      "stdout": {
        "human": "no-command-result"
      }
    }
  ]
}
```

### `/external_contract/cli/gogurt/commands/watch/terminating_controls`

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
