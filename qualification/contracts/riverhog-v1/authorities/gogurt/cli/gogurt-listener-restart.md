# gogurt listener restart

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-restart:056188764d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-ac32d59250"></a>Parser name: `restart`
- <a id="s-049ea383ad"></a>Extra arguments at this parser: rejected.
- <a id="s-e1bfd6e91c"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-09262c4f86"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-dbe5643cac"></a>`listener_host_provider`<br>`--listener-host-provider` | optional option; 1 value | text | not recorded<br>Env: `"GOGURT_LISTENER_HOST_PROVIDER"` |
| <a id="s-da26c387d5"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c93e0638b4"></a>`help` | <a id="s-1f34317d7b"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-8c180212e0"></a>`0` | <a id="s-e8e9cd4417"></a>`"noncontractual-framework-help"` | <a id="s-9bd2a076a6"></a>`"empty"` |

### Result and failure contract

- <a id="s-635cf9b58d"></a>Result identity: `gogurt-cli-result/listener/restart/v1`
- <a id="s-45c0f415a7"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-f7148aeecd"></a>Structured output: `optional-json`
- <a id="s-a63f64cb84"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-2c0740f018"></a>`completed` | <a id="s-469b078914"></a>`{"kind":"command-completed"}` | <a id="s-26bfa21678"></a>`0` | <a id="s-d25d2e8000"></a>human: `"noncontractual-presentation-of-command-result"`; json: [gogurt-listener-status/v1](#s-f4e0f53bf4) | <a id="s-f05a5c9f17"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-3e30a69b6d"></a>`usage` | <a id="s-fc4cc09c67"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-f0aa8522e9"></a>`2` | <a id="s-8f434a665f"></a>all: `"empty"` | <a id="s-c29092f5ee"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-e7943d4ce9"></a>`operational` | <a id="s-690483c3de"></a>`{"kind":"application-error"}` | <a id="s-ed44622329"></a>`1` | <a id="s-d38046a23f"></a>human: `"empty"`; json: [gogurt-cli-error/v1](#s-4a68dabf11) | <a id="s-b8a39e506e"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-f4e0f53bf4"></a>`gogurt-listener-status/v1`

Applies to: completed · stdout (json).

<a id="s-1b2d8b1976"></a>

- <a id="s-bc08e198ab"></a>`type`: `"object"`
- <a id="s-876f88e28f"></a>`additionalProperties`: `false`
- <a id="s-6b1fc6e8ab"></a>`required`: `["schema","manager_version","platform","installed","enabled","running","health","config_file","state_dir","executable","mounted_volume_provider","listener_host_provider","heartbeat_age_seconds","heartbeat","dispatches","mount_attention","diagnostic"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5192451d32"></a>`config_file` | yes | type="string" |  |
| <a id="s-d3bb72efc3"></a>`diagnostic` | yes | type=["string","null"] |  |
| <a id="s-96bcd1e788"></a>`dispatches` | yes | type="object" |  |
| <a id="s-2d44d7d670"></a>`enabled` | yes | type="boolean" |  |
| <a id="s-67ebb5552e"></a>`executable` | yes | type=["string","null"] |  |
| <a id="s-548555546e"></a>`health` | yes | enum=["absent","stopped","failed","starting","healthy","stale"] |  |
| <a id="s-d5eefc9d94"></a>`heartbeat` | yes | type=["object","null"] |  |
| <a id="s-c6711a267d"></a>`heartbeat_age_seconds` | yes | type=["number","null"]; minimum=0 |  |
| <a id="s-72a80a24bc"></a>`installed` | yes | type="boolean" |  |
| `listener_host_provider` | yes | [See field `listener_host_provider`](#s-bb7127b02a) |  |
| <a id="s-2e97b84f71"></a>`manager_version` | yes | type="string" |  |
| <a id="s-374708a007"></a>`mount_attention` | yes | type="array" |  |
| `mounted_volume_provider` | yes | [See field `mounted_volume_provider`](#s-9aada13f95) |  |
| <a id="s-7057a2c19c"></a>`platform` | yes | type="string" |  |
| <a id="s-b3a3eb6bd4"></a>`running` | yes | type="boolean" |  |
| <a id="s-1facab724c"></a>`schema` | yes | const="gogurt-listener-status/v1" |  |
| <a id="s-de704ee0ae"></a>`state_dir` | yes | type="string" |  |

##### <a id="s-bb7127b02a"></a>field `listener_host_provider`


###### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| 1 | [See field `listener_host_provider` · `anyOf` alternative 1](#s-4a4cdb081f) |
| <a id="s-76bbc7e605"></a>2 | type="null" |

##### <a id="s-9aada13f95"></a>field `mounted_volume_provider`


###### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| 1 | [See field `mounted_volume_provider` · `anyOf` alternative 1](#s-5f91a6217f) |
| <a id="s-925ab4dafd"></a>2 | type="null" |

##### <a id="s-4a4cdb081f"></a>field `listener_host_provider` · `anyOf` alternative 1

- <a id="s-803b6d1f34"></a>`type`: `"object"`
- <a id="s-48d495660e"></a>`additionalProperties`: `false`
- <a id="s-6dbd16717a"></a>`required`: `["kind","name","provider_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d4a8c9cfc1"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-880542b785"></a>`name` | yes | type="string" |  |
| <a id="s-0f9766ae63"></a>`provider_id` | yes | type="string" |  |

##### <a id="s-5f91a6217f"></a>field `mounted_volume_provider` · `anyOf` alternative 1

- <a id="s-4871733348"></a>`type`: `"object"`
- <a id="s-46efa1ac59"></a>`additionalProperties`: `false`
- <a id="s-a523e6f539"></a>`required`: `["kind","name","provider_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a17e456abd"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-dce2e48a97"></a>`name` | yes | type="string" |  |
| <a id="s-077435f840"></a>`provider_id` | yes | type="string" |  |

#### <a id="s-4a68dabf11"></a>`gogurt-cli-error/v1`

Applies to: operational · stdout (json).

<a id="s-596cea1769"></a>

- <a id="s-4189b8b361"></a>`type`: `"object"`
- <a id="s-18bbd3619f"></a>`additionalProperties`: `false`
- <a id="s-b9a06e7187"></a>`required`: `["error"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | [See field `error`](#s-3f2b3050d7) |  |

##### <a id="s-3f2b3050d7"></a>field `error`

- <a id="s-e7ecf17158"></a>`type`: `"object"`
- <a id="s-21fc02cc0b"></a>`additionalProperties`: `false`
- <a id="s-37043c11d5"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ab5000b382"></a>`code` | yes | enum=["config_error","listener_error"] |  |
| <a id="s-b5e8ce93c6"></a>`message` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-da26c387d5) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --listener-host-provider](#s-dbe5643cac) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-df6f3d3735"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-b28e4d66fb"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources/authorities.md#src-3b2297c37d) — [reference/gogurt/application/src/gogurt/cli.py::&lt;module&gt;](../../../../../../reference/gogurt/application/src/gogurt/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/restart/allow_extra_args`
- `/external_contract/cli/gogurt/commands/listener/commands/restart/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/listener/commands/restart/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/listener/commands/restart/name`
- `/external_contract/cli/gogurt/commands/listener/commands/restart/parameters`
- `/external_contract/cli/gogurt/commands/listener/commands/restart/result_contract`
- `/external_contract/cli/gogurt/commands/listener/commands/restart/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/listener/commands/restart/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/listener/commands/restart/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/listener/commands/restart/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/listener/commands/restart/name`

<!-- exact-contract-value: 84841d3a0e2418b75dec3d5df55954803d61a26a4cd821d8df25908486b04a55 -->

```json
"restart"
```

### `/external_contract/cli/gogurt/commands/listener/commands/restart/parameters`

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

### `/external_contract/cli/gogurt/commands/listener/commands/restart/result_contract`

<!-- exact-contract-value: 06acef61a497a4b3942df9d6be3d2d33550ef6be4dca0ba09f21bd3978df73c2 -->

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
  "identity": "gogurt-cli-result/listener/restart/v1",
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

### `/external_contract/cli/gogurt/commands/listener/commands/restart/terminating_controls`

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
