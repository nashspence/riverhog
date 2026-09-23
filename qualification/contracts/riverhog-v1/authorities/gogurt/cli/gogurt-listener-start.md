# gogurt listener start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-start:098276da87 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-b1de582a6c"></a>Parser name: `start`
- <a id="s-17720c58ac"></a>Extra arguments at this parser: rejected.
- <a id="s-88d2381ed3"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-929deceba3"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-803a3ef3e9"></a>`listener_host_provider`<br>`--listener-host-provider` | optional option; 1 value | text | not recorded<br>Env: `"GOGURT_LISTENER_HOST_PROVIDER"` |
| <a id="s-ee7d3bdc45"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-300411789e"></a>`help` | <a id="s-dbc60c1b2b"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-29409a675c"></a>`0` | <a id="s-3fbbb1ac89"></a>`"noncontractual-framework-help"` | <a id="s-36d273bdf8"></a>`"empty"` |

### Result and failure contract

- <a id="s-de0497bbc6"></a>Result identity: `gogurt-cli-result/listener/start/v1`
- <a id="s-67e71b44ed"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-fe7e19d4f9"></a>Structured output: `optional-json`
- <a id="s-a512eb3ec4"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6500d7576d"></a>`completed` | <a id="s-8722664e3b"></a>`{"kind":"command-completed"}` | <a id="s-9055574a5b"></a>`0` | <a id="s-ce27145fb6"></a>human: `"noncontractual-presentation-of-command-result"`; json: [gogurt-listener-status/v1](#s-dd89f8147a) | <a id="s-9fec4eff17"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-61af2ef932"></a>`usage` | <a id="s-2a4cd57070"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-0b83952898"></a>`2` | <a id="s-2d1cddc1e0"></a>all: `"empty"` | <a id="s-951938b362"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-652373259d"></a>`operational` | <a id="s-259bf0b23d"></a>`{"kind":"application-error"}` | <a id="s-055e1b2724"></a>`1` | <a id="s-2e3213e14e"></a>human: `"empty"`; json: [gogurt-cli-error/v1](#s-f3033ab4e7) | <a id="s-49ba0ac894"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-dd89f8147a"></a>`gogurt-listener-status/v1`

Applies to: completed · stdout (json).

<a id="s-35c4b3e0e4"></a>

- <a id="s-07f33f5fda"></a>`type`: `"object"`
- <a id="s-cc91c1ba7c"></a>`additionalProperties`: `false`
- <a id="s-bdb5354a1f"></a>`required`: `["schema","manager_version","platform","installed","enabled","running","health","config_file","state_dir","executable","mounted_volume_provider","listener_host_provider","heartbeat_age_seconds","heartbeat","dispatches","mount_attention","diagnostic"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d286ec4751"></a>`config_file` | yes | type="string" |  |
| <a id="s-200820e0ee"></a>`diagnostic` | yes | type=["string","null"] |  |
| <a id="s-a5724423d7"></a>`dispatches` | yes | type="object" |  |
| <a id="s-c3ab04d3c5"></a>`enabled` | yes | type="boolean" |  |
| <a id="s-89127ef581"></a>`executable` | yes | type=["string","null"] |  |
| <a id="s-0dfae8ed16"></a>`health` | yes | enum=["absent","stopped","failed","starting","healthy","stale"] |  |
| <a id="s-69be3fbf23"></a>`heartbeat` | yes | type=["object","null"] |  |
| <a id="s-1f4be22fac"></a>`heartbeat_age_seconds` | yes | type=["number","null"]; minimum=0 |  |
| <a id="s-d7688ce469"></a>`installed` | yes | type="boolean" |  |
| `listener_host_provider` | yes | [See field `listener_host_provider`](#s-68bdffbeb2) |  |
| <a id="s-b3745fec74"></a>`manager_version` | yes | type="string" |  |
| <a id="s-9403eff859"></a>`mount_attention` | yes | type="array" |  |
| `mounted_volume_provider` | yes | [See field `mounted_volume_provider`](#s-30603c1204) |  |
| <a id="s-a08301126c"></a>`platform` | yes | type="string" |  |
| <a id="s-cd07864b78"></a>`running` | yes | type="boolean" |  |
| <a id="s-3579ae25e2"></a>`schema` | yes | const="gogurt-listener-status/v1" |  |
| <a id="s-df652ddc75"></a>`state_dir` | yes | type="string" |  |

##### <a id="s-68bdffbeb2"></a>field `listener_host_provider`


###### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| 1 | [See field `listener_host_provider` · `anyOf` alternative 1](#s-e56a79c50b) |
| <a id="s-7e4a900647"></a>2 | type="null" |

##### <a id="s-30603c1204"></a>field `mounted_volume_provider`


###### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| 1 | [See field `mounted_volume_provider` · `anyOf` alternative 1](#s-ced513c505) |
| <a id="s-00120ad322"></a>2 | type="null" |

##### <a id="s-e56a79c50b"></a>field `listener_host_provider` · `anyOf` alternative 1

- <a id="s-4f68fb179e"></a>`type`: `"object"`
- <a id="s-3873a21e2a"></a>`additionalProperties`: `false`
- <a id="s-609634a917"></a>`required`: `["kind","name","provider_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4a39abebcf"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-5dd6876817"></a>`name` | yes | type="string" |  |
| <a id="s-d30f01aeec"></a>`provider_id` | yes | type="string" |  |

##### <a id="s-ced513c505"></a>field `mounted_volume_provider` · `anyOf` alternative 1

- <a id="s-3a503c3652"></a>`type`: `"object"`
- <a id="s-9643fc78f9"></a>`additionalProperties`: `false`
- <a id="s-c096435b04"></a>`required`: `["kind","name","provider_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7689db4acf"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-d1104c7e55"></a>`name` | yes | type="string" |  |
| <a id="s-0ab37ff1d5"></a>`provider_id` | yes | type="string" |  |

#### <a id="s-f3033ab4e7"></a>`gogurt-cli-error/v1`

Applies to: operational · stdout (json).

<a id="s-4c39d9c95c"></a>

- <a id="s-063ad4a5fd"></a>`type`: `"object"`
- <a id="s-9f64fce32d"></a>`additionalProperties`: `false`
- <a id="s-a60201ae51"></a>`required`: `["error"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | [See field `error`](#s-d0b3a44a54) |  |

##### <a id="s-d0b3a44a54"></a>field `error`

- <a id="s-2eaefe55f3"></a>`type`: `"object"`
- <a id="s-f5edef74bd"></a>`additionalProperties`: `false`
- <a id="s-f0b1942a29"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-31b2edaf6b"></a>`code` | yes | enum=["config_error","listener_error"] |  |
| <a id="s-74c957a3d3"></a>`message` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-ee7d3bdc45) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --listener-host-provider](#s-803a3ef3e9) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-461e6c87af"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-6355369719"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources/authorities.md#src-3b2297c37d) — [some-implementations/gogurt/application/src/gogurt/cli.py::&lt;module&gt;](../../../../../../some-implementations/gogurt/application/src/gogurt/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/start/allow_extra_args`
- `/external_contract/cli/gogurt/commands/listener/commands/start/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/listener/commands/start/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/listener/commands/start/name`
- `/external_contract/cli/gogurt/commands/listener/commands/start/parameters`
- `/external_contract/cli/gogurt/commands/listener/commands/start/result_contract`
- `/external_contract/cli/gogurt/commands/listener/commands/start/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/listener/commands/start/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/listener/commands/start/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/listener/commands/start/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/listener/commands/start/name`

<!-- exact-contract-value: a92ae9615600f7f0bcb0edf9703b379c163bef33ed749ae40c48a0830d4ab6ae -->

```json
"start"
```

### `/external_contract/cli/gogurt/commands/listener/commands/start/parameters`

<!-- exact-contract-value: fe7b56d2906ac79e76d31abc11e3bbc7c7ed0b66ee89c57133a60c8c1242d2fb -->

```json
[
  {
    "count": false,
    "envvar": "GOGURT_LISTENER_HOST_PROVIDER",
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "listener_host_provider",
    "nargs": 1,
    "options": [
      "--listener-host-provider"
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

### `/external_contract/cli/gogurt/commands/listener/commands/start/result_contract`

<!-- exact-contract-value: 5f6100b95904ef300c895d29e33fd44d660ce8b6f26a6d9e1114e4b8b03aa5c5 -->

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
  "identity": "gogurt-cli-result/listener/start/v1",
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
          "identity": "gogurt-listener-status/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "config_file": {
                "type": "string"
              },
              "diagnostic": {
                "type": [
                  "string",
                  "null"
                ]
              },
              "dispatches": {
                "type": "object"
              },
              "enabled": {
                "type": "boolean"
              },
              "executable": {
                "type": [
                  "string",
                  "null"
                ]
              },
              "health": {
                "enum": [
                  "absent",
                  "stopped",
                  "failed",
                  "starting",
                  "healthy",
                  "stale"
                ]
              },
              "heartbeat": {
                "type": [
                  "object",
                  "null"
                ]
              },
              "heartbeat_age_seconds": {
                "minimum": 0,
                "type": [
                  "number",
                  "null"
                ]
              },
              "installed": {
                "type": "boolean"
              },
              "listener_host_provider": {
                "anyOf": [
                  {
                    "additionalProperties": false,
                    "properties": {
                      "kind": {
                        "enum": [
                          "mounted-volume",
                          "listener-host"
                        ]
                      },
                      "name": {
                        "type": "string"
                      },
                      "provider_id": {
                        "type": "string"
                      }
                    },
                    "required": [
                      "kind",
                      "name",
                      "provider_id"
                    ],
                    "type": "object"
                  },
                  {
                    "type": "null"
                  }
                ]
              },
              "manager_version": {
                "type": "string"
              },
              "mount_attention": {
                "type": "array"
              },
              "mounted_volume_provider": {
                "anyOf": [
                  {
                    "additionalProperties": false,
                    "properties": {
                      "kind": {
                        "enum": [
                          "mounted-volume",
                          "listener-host"
                        ]
                      },
                      "name": {
                        "type": "string"
                      },
                      "provider_id": {
                        "type": "string"
                      }
                    },
                    "required": [
                      "kind",
                      "name",
                      "provider_id"
                    ],
                    "type": "object"
                  },
                  {
                    "type": "null"
                  }
                ]
              },
              "platform": {
                "type": "string"
              },
              "running": {
                "type": "boolean"
              },
              "schema": {
                "const": "gogurt-listener-status/v1"
              },
              "state_dir": {
                "type": "string"
              }
            },
            "required": [
              "schema",
              "manager_version",
              "platform",
              "installed",
              "enabled",
              "running",
              "health",
              "config_file",
              "state_dir",
              "executable",
              "mounted_volume_provider",
              "listener_host_provider",
              "heartbeat_age_seconds",
              "heartbeat",
              "dispatches",
              "mount_attention",
              "diagnostic"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/gogurt/commands/listener/commands/start/terminating_controls`

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
