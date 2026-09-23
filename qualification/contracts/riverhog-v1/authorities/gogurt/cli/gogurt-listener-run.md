# gogurt listener _run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-run:445c96f501 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-fb1d923bfc"></a>Parser name: `_run`
- <a id="s-ab8b1a160c"></a>Extra arguments at this parser: rejected.
- <a id="s-4305afeadc"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-e5b9d7bbbb"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-0c097bf54d"></a>`runtime_config`<br>`--runtime-config` | required option; 1 value | path; existence not required; regular files allowed; directories allowed; access checks on existing paths: read; resolve absolute path and symlinks: no; dash uses normal path checks | not recorded<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c22d909738"></a>`help` | <a id="s-ca62cd5afe"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-5f03758741"></a>`0` | <a id="s-7ccc60c4cf"></a>`"noncontractual-framework-help"` | <a id="s-b098a6073a"></a>`"empty"` |

### Result and failure contract

- <a id="s-0953e99ed7"></a>Result identity: `gogurt-cli-result/listener/_run/v1`
- <a id="s-100697cec4"></a>Profile: `gogurt-cli-listener-runtime/v1`
- <a id="s-b13021d8a5"></a>Structured output: `none`
- <a id="s-a8b4591f12"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b8d539a5aa"></a>`stopped` | <a id="s-a142542f10"></a>`{"kind":"listener-runtime-returned"}` | <a id="s-826785cebb"></a>`0` | <a id="s-45f82f8b45"></a>human: `"no-command-result"` | <a id="s-d9fc2fe84c"></a>human: `"noncontractual-runtime-status"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-70bf80372a"></a>`usage` | <a id="s-5646576a32"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-6d66a7e4f8"></a>`2` | <a id="s-8baffb6201"></a>human: `"empty"` | <a id="s-3937824054"></a>human: `"noncontractual-usage-diagnostic"` |
| <a id="s-f9b0a7cbde"></a>`operational` | <a id="s-ee0bfcdbc6"></a>`{"kind":"application-error"}` | <a id="s-dbce6f4e51"></a>`1` | <a id="s-e4a64afda9"></a>human: `"empty"` | <a id="s-d26e9b641e"></a>human: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --runtime-config](#s-0c097bf54d) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3912b14bd5"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-e1d4dda27e"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources/authorities.md#src-3b2297c37d) — [some-implementations/gogurt/application/src/gogurt/cli.py::&lt;module&gt;](../../../../../../some-implementations/gogurt/application/src/gogurt/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/_run/allow_extra_args`
- `/external_contract/cli/gogurt/commands/listener/commands/_run/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/listener/commands/_run/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/listener/commands/_run/name`
- `/external_contract/cli/gogurt/commands/listener/commands/_run/parameters`
- `/external_contract/cli/gogurt/commands/listener/commands/_run/result_contract`
- `/external_contract/cli/gogurt/commands/listener/commands/_run/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/listener/commands/_run/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/listener/commands/_run/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/listener/commands/_run/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/listener/commands/_run/name`

<!-- exact-contract-value: 5f0b1cc0fff7e256dbce362a64c00555715bcd2eb6cd0040718cee353633a7ff -->

```json
"_run"
```

### `/external_contract/cli/gogurt/commands/listener/commands/_run/parameters`

<!-- exact-contract-value: dd37343082f2804cd2c238266fa0b51ec261292077c8a608015b266c5fc79f36 -->

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
  }
]
```

### `/external_contract/cli/gogurt/commands/listener/commands/_run/result_contract`

<!-- exact-contract-value: eb5fc841cbf635e211bc31e89d0f3c8df0ad4813072a7bcd7ef1f1a0878becc3 -->

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
  "identity": "gogurt-cli-result/listener/_run/v1",
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

### `/external_contract/cli/gogurt/commands/listener/commands/_run/terminating_controls`

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
