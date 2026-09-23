# gogurt list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-list:1600e8fba5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-df82c9962d"></a>Parser name: `list`
- <a id="s-fe9815c6d8"></a>Extra arguments at this parser: rejected.
- <a id="s-ba6b90807c"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-f55b5fface"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-64caa42d6e"></a>`config`<br>`--config` | optional option; 1 value | path; existence not required; regular files allowed; directories allowed; access checks on existing paths: read; resolve absolute path and symlinks: no; dash uses normal path checks | not recorded<br>Env: `null` |
| <a id="s-52df517a50"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-9d5fbe3ea1"></a>`help` | <a id="s-dad554dec1"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-b475a3c5ee"></a>`0` | <a id="s-8b65bb9175"></a>`"noncontractual-framework-help"` | <a id="s-3ce94021ff"></a>`"empty"` |

### Result and failure contract

- <a id="s-cf186f3484"></a>Result identity: `gogurt-cli-result/list/v1`
- <a id="s-1a81606541"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-5a46b737f0"></a>Structured output: `optional-json`
- <a id="s-90ba37a07a"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c3a2f34fe6"></a>`completed` | <a id="s-9be9b36324"></a>`{"kind":"command-completed"}` | <a id="s-9b9daa79b4"></a>`0` | <a id="s-80284ed44b"></a>human: `"noncontractual-presentation-of-command-result"`; json: [gogurt-route-list/v1](#s-9fb11d9a1a) | <a id="s-51c0033cdf"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0dfa26dacd"></a>`usage` | <a id="s-170d4633cf"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-486ef488cb"></a>`2` | <a id="s-ebbf4ae230"></a>all: `"empty"` | <a id="s-65dd649f1a"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-9f9c616a50"></a>`operational` | <a id="s-4fe71ed413"></a>`{"kind":"application-error"}` | <a id="s-7b85f4007b"></a>`1` | <a id="s-9130a5c532"></a>human: `"empty"`; json: [gogurt-cli-error/v1](#s-23228a8dba) | <a id="s-a16876af5c"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-9fb11d9a1a"></a>`gogurt-route-list/v1`

Applies to: completed · stdout (json).

<a id="s-9ecf080440"></a>

- <a id="s-bc51deb513"></a>`type`: `"array"`
- `items`: [See `items`](#s-c5bdd8a5b1)

##### <a id="s-c5bdd8a5b1"></a>`items`

- <a id="s-76bfb0d1b0"></a>`type`: `"object"`
- <a id="s-24312bf97d"></a>`additionalProperties`: `false`
- <a id="s-55f0042528"></a>`required`: `["route","command"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-43927f6d85"></a>`command` | yes | type="array"; items=(type="string") |  |
| <a id="s-279f5e1689"></a>`route` | yes | type="string" |  |

#### <a id="s-23228a8dba"></a>`gogurt-cli-error/v1`

Applies to: operational · stdout (json).

<a id="s-afebef9c8b"></a>

- <a id="s-0837872786"></a>`type`: `"object"`
- <a id="s-911d30c352"></a>`additionalProperties`: `false`
- <a id="s-79b10fb078"></a>`required`: `["error"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | [See field `error`](#s-9e8ba42bfe) |  |

##### <a id="s-9e8ba42bfe"></a>field `error`

- <a id="s-58e024c722"></a>`type`: `"object"`
- <a id="s-6b4bcb0dfc"></a>`additionalProperties`: `false`
- <a id="s-34c68a05c9"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8a53e8c656"></a>`code` | yes | enum=["config_error","listener_error"] |  |
| <a id="s-bb1644b7e8"></a>`message` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --config](#s-64caa42d6e) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-52df517a50) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b94ad11697"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-b9d8daac6d"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources/authorities.md#src-3b2297c37d) — [reference/gogurt/application/src/gogurt/cli.py::&lt;module&gt;](../../../../../../reference/gogurt/application/src/gogurt/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/gogurt/commands/list/allow_extra_args`
- `/external_contract/cli/gogurt/commands/list/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/list/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/list/name`
- `/external_contract/cli/gogurt/commands/list/parameters`
- `/external_contract/cli/gogurt/commands/list/result_contract`
- `/external_contract/cli/gogurt/commands/list/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/list/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/list/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/list/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/gogurt/commands/list/parameters`

<!-- exact-contract-value: ab745f30a64f8ef8aa28769c8709b0ec0355375abc829a32f3d8b3cfbbb2f47b -->

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

### `/external_contract/cli/gogurt/commands/list/result_contract`

<!-- exact-contract-value: 3014e520a204d7dc2c74c12245309d014a8dff65e690fe13a29dd0ebae36549f -->

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
        "all": "noncontractual-usage-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    },
    {
      "exit_status": 1,
      "id": "operational",
      "selected_by": {
        "kind": "application-error"
      },
      "stderr": {
        "human": "noncontractual-diagnostic",
        "json": "empty"
      },
      "stdout": {
        "human": "empty",
        "json": {
          "identity": "gogurt-cli-error/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "error": {
                "additionalProperties": false,
                "properties": {
                  "code": {
                    "enum": [
                      "config_error",
                      "listener_error"
                    ]
                  },
                  "message": {
                    "type": "string"
                  }
                },
                "required": [
                  "code",
                  "message"
                ],
                "type": "object"
              }
            },
            "required": [
              "error"
            ],
            "type": "object"
          }
        }
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "gogurt-cli-result/list/v1",
  "profile_id": "gogurt-cli-human-json/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "completed",
      "selected_by": {
        "kind": "command-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "identity": "gogurt-route-list/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "items": {
              "additionalProperties": false,
              "properties": {
                "command": {
                  "items": {
                    "type": "string"
                  },
                  "type": "array"
                },
                "route": {
                  "type": "string"
                }
              },
              "required": [
                "route",
                "command"
              ],
              "type": "object"
            },
            "type": "array"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/gogurt/commands/list/terminating_controls`

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
