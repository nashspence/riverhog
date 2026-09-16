# gogurt listener uninstall

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-uninstall:db7855987d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-bf49e85607"></a>Parser name: `uninstall`
- <a id="s-c24ce9b251"></a>Extra arguments at this parser: rejected.
- <a id="s-0bc2cc04fb"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-dd8817f4e7"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-9bf6e52d46"></a>`listener_host_provider`<br>`--listener-host-provider` | optional option; 1 value | text | not recorded<br>Env: `"GOGURT_LISTENER_HOST_PROVIDER"` |
| <a id="s-9f84e8d6b1"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-f99533d9e1"></a>`help` | <a id="s-c3482ef18a"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-a37b25cb27"></a>`0` | <a id="s-ef6578ac86"></a>`"noncontractual-framework-help"` | <a id="s-f2150ba02b"></a>`"empty"` |

### Result and failure contract

- <a id="s-7309472bd3"></a>Result identity: `gogurt-cli-result/listener/uninstall/v1`
- <a id="s-3845455ef3"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-4aad861e0c"></a>Structured output: `optional-json`
- <a id="s-a1c234178f"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-3d05495670"></a>`completed` | <a id="s-6a33c82bf9"></a>`{"kind":"command-completed"}` | <a id="s-b5bb63891f"></a>`0` | <a id="s-f498ce6822"></a>human: `noncontractual-presentation-of-command-result`; json: [gogurt-listener-status/v1](#s-808688b435) | <a id="s-2752e77382"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a145130e57"></a>`usage` | <a id="s-9c6f31135e"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-1efb38269b"></a>`2` | <a id="s-c32269de3c"></a>all: `empty` | <a id="s-17bfb09a33"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-af71b8dcf5"></a>`operational` | <a id="s-24673a8203"></a>`{"kind":"application-error"}` | <a id="s-33bfc36d90"></a>`1` | <a id="s-5f08400698"></a>human: `empty`; json: [gogurt-cli-error/v1](#s-9fe4702937) | <a id="s-2428b3eefe"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Local structured outputs


#### <a id="s-808688b435"></a>`gogurt-listener-status/v1`

Applies to: completed · stdout (json).

<a id="s-75e82de43f"></a>

- <a id="s-0592172b7f"></a>`type`: `"object"`
- <a id="s-c2a704414d"></a>`additionalProperties`: `false`
- <a id="s-6adbedf19c"></a>`required`: `["schema","manager_version","platform","installed","enabled","running","health","config_file","state_dir","executable","mounted_volume_provider","listener_host_provider","heartbeat_age_seconds","heartbeat","dispatches","mount_attention","diagnostic"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a5f8d05e6b"></a>`config_file` | yes | type="string" |  |
| <a id="s-4de011a5f4"></a>`diagnostic` | yes | type=["string","null"] |  |
| <a id="s-c4bfb8640f"></a>`dispatches` | yes | type="object" |  |
| <a id="s-1c2d6b6619"></a>`enabled` | yes | type="boolean" |  |
| <a id="s-e686a10680"></a>`executable` | yes | type=["string","null"] |  |
| <a id="s-ac2d199437"></a>`health` | yes | enum=["absent","stopped","failed","starting","healthy","stale"] |  |
| <a id="s-952b18b769"></a>`heartbeat` | yes | type=["object","null"] |  |
| <a id="s-382c2b8724"></a>`heartbeat_age_seconds` | yes | type=["number","null"]; minimum=0 |  |
| <a id="s-20f4c81ffc"></a>`installed` | yes | type="boolean" |  |
| `listener_host_provider` | yes | [See field `listener_host_provider`](#s-7955cca4e4) |  |
| <a id="s-6e3c192af2"></a>`manager_version` | yes | type="string" |  |
| <a id="s-8789edbecd"></a>`mount_attention` | yes | type="array" |  |
| `mounted_volume_provider` | yes | [See field `mounted_volume_provider`](#s-fbbad21b6d) |  |
| <a id="s-c7ef02da68"></a>`platform` | yes | type="string" |  |
| <a id="s-77e6d94bc9"></a>`running` | yes | type="boolean" |  |
| <a id="s-f17b6a478e"></a>`schema` | yes | const="gogurt-listener-status/v1" |  |
| <a id="s-2dc9f4f8c4"></a>`state_dir` | yes | type="string" |  |

##### <a id="s-7955cca4e4"></a>field `listener_host_provider`


###### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| 1 | [See field `listener_host_provider` · `anyOf` alternative 1](#s-fa12e18065) |
| <a id="s-2fe23eea82"></a>2 | type="null" |

##### <a id="s-fbbad21b6d"></a>field `mounted_volume_provider`


###### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| 1 | [See field `mounted_volume_provider` · `anyOf` alternative 1](#s-ee7f6a2a35) |
| <a id="s-062981577e"></a>2 | type="null" |

##### <a id="s-fa12e18065"></a>field `listener_host_provider` · `anyOf` alternative 1

- <a id="s-7ea82be3c3"></a>`type`: `"object"`
- <a id="s-73fd22068c"></a>`additionalProperties`: `false`
- <a id="s-b573a4403a"></a>`required`: `["kind","name","provider_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ab88b79dee"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-04ef85fe90"></a>`name` | yes | type="string" |  |
| <a id="s-704373cc20"></a>`provider_id` | yes | type="string" |  |

##### <a id="s-ee7f6a2a35"></a>field `mounted_volume_provider` · `anyOf` alternative 1

- <a id="s-1dfe723ac4"></a>`type`: `"object"`
- <a id="s-c8fb8cb92b"></a>`additionalProperties`: `false`
- <a id="s-0edcbb7adb"></a>`required`: `["kind","name","provider_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fabe34893c"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-d5ab41c876"></a>`name` | yes | type="string" |  |
| <a id="s-fcf943f4bf"></a>`provider_id` | yes | type="string" |  |

#### <a id="s-9fe4702937"></a>`gogurt-cli-error/v1`

Applies to: operational · stdout (json).

<a id="s-fe5ce4193d"></a>

- <a id="s-ee6f81fac5"></a>`type`: `"object"`
- <a id="s-0f9590d29b"></a>`additionalProperties`: `false`
- <a id="s-e10ba85acf"></a>`required`: `["error"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | [See field `error`](#s-66bbaa470c) |  |

##### <a id="s-66bbaa470c"></a>field `error`

- <a id="s-82328c9780"></a>`type`: `"object"`
- <a id="s-37e2d31c49"></a>`additionalProperties`: `false`
- <a id="s-893ac64401"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7da4d99ebe"></a>`code` | yes | enum=["config_error","listener_error"] |  |
| <a id="s-e5fcd18171"></a>`message` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-9f84e8d6b1) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --listener-host-provider](#s-9bf6e52d46) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Governing policies

- <a id="pa-8ee322b8f0"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-3bc090fde4"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/uninstall/allow_extra_args`
- `/external_contract/cli/gogurt/commands/listener/commands/uninstall/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/listener/commands/uninstall/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/listener/commands/uninstall/name`
- `/external_contract/cli/gogurt/commands/listener/commands/uninstall/parameters`
- `/external_contract/cli/gogurt/commands/listener/commands/uninstall/result_contract`
- `/external_contract/cli/gogurt/commands/listener/commands/uninstall/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/listener/commands/uninstall/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/listener/commands/uninstall/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/listener/commands/uninstall/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/listener/commands/uninstall/name`

<!-- exact-contract-value: 40fdac7cdc15fc93453dfc8a21ab0ff042c64c52dfaac794da0e9f625d9357a0 -->

```json
"uninstall"
```

### `/external_contract/cli/gogurt/commands/listener/commands/uninstall/parameters`

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

### `/external_contract/cli/gogurt/commands/listener/commands/uninstall/result_contract`

<!-- exact-contract-value: f53932b67963e5e36070f3e8442a55f66d6fed6f127faf98bd2252fe70567254 -->

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
  "identity": "gogurt-cli-result/listener/uninstall/v1",
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

### `/external_contract/cli/gogurt/commands/listener/commands/uninstall/terminating_controls`

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
