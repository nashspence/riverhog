# gogurt listener status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-status:e544589602 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-dea1d56be9"></a>Parser name: `status`
- <a id="s-aa3076b490"></a>Extra arguments at this parser: rejected.
- <a id="s-f2699dd328"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-5a3d2e54f2"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-47e3cfdefa"></a>`listener_host_provider`<br>`--listener-host-provider` | optional option; 1 value | text | not recorded<br>Env: `"GOGURT_LISTENER_HOST_PROVIDER"` |
| <a id="s-2e525796ff"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-96f4bda58b"></a>`help` | <a id="s-0aa6004567"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-0658e513e0"></a>`0` | <a id="s-20d524f9e3"></a>`"noncontractual-framework-help"` | <a id="s-19dd283f16"></a>`"empty"` |

### Result and failure contract

- <a id="s-c85205582a"></a>Result identity: `gogurt-cli-result/listener/status/v1`
- <a id="s-1529ba2f55"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-8994b840cb"></a>Structured output: `optional-json`
- <a id="s-6662b28e5a"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5a5305db12"></a>`completed` | <a id="s-32f5519ef9"></a>`{"kind":"command-completed"}` | <a id="s-89e35e5dbf"></a>`0` | <a id="s-be29598711"></a>human: `"noncontractual-presentation-of-command-result"`; json: [gogurt-listener-status/v1](#s-2c5935f6ef) | <a id="s-0c65c9fd90"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c1f67c0017"></a>`usage` | <a id="s-3aa709d1a0"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-4358d99190"></a>`2` | <a id="s-c5341bf6ae"></a>all: `"empty"` | <a id="s-db8f1f754c"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-6c2cb2d2f3"></a>`operational` | <a id="s-505cf6924b"></a>`{"kind":"application-error"}` | <a id="s-20b346ac94"></a>`1` | <a id="s-eb862390d6"></a>human: `"empty"`; json: [gogurt-cli-error/v1](#s-7e6ce2c365) | <a id="s-36936a865b"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-2c5935f6ef"></a>`gogurt-listener-status/v1`

Applies to: completed · stdout (json).

<a id="s-fd3087dda1"></a>

- <a id="s-f4fc96b8be"></a>`type`: `"object"`
- <a id="s-af3d39f9bb"></a>`additionalProperties`: `false`
- <a id="s-9f0efd742e"></a>`required`: `["schema","manager_version","platform","installed","enabled","running","health","config_file","state_dir","executable","mounted_volume_provider","listener_host_provider","heartbeat_age_seconds","heartbeat","dispatches","mount_attention","diagnostic"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b49b2ef831"></a>`config_file` | yes | type="string" |  |
| <a id="s-16a4d82c13"></a>`diagnostic` | yes | type=["string","null"] |  |
| <a id="s-082d4a5c9d"></a>`dispatches` | yes | type="object" |  |
| <a id="s-1522723cf0"></a>`enabled` | yes | type="boolean" |  |
| <a id="s-98933d8c17"></a>`executable` | yes | type=["string","null"] |  |
| <a id="s-fa105f8a94"></a>`health` | yes | enum=["absent","stopped","failed","starting","healthy","stale"] |  |
| <a id="s-746ce980ca"></a>`heartbeat` | yes | type=["object","null"] |  |
| <a id="s-8674d92314"></a>`heartbeat_age_seconds` | yes | type=["number","null"]; minimum=0 |  |
| <a id="s-47c47ebf68"></a>`installed` | yes | type="boolean" |  |
| `listener_host_provider` | yes | [See field `listener_host_provider`](#s-b2e78b5b65) |  |
| <a id="s-33b8b5be4d"></a>`manager_version` | yes | type="string" |  |
| <a id="s-942a11b9d9"></a>`mount_attention` | yes | type="array" |  |
| `mounted_volume_provider` | yes | [See field `mounted_volume_provider`](#s-fc31c919f9) |  |
| <a id="s-5d22608978"></a>`platform` | yes | type="string" |  |
| <a id="s-3ddce5a46e"></a>`running` | yes | type="boolean" |  |
| <a id="s-4f05339d42"></a>`schema` | yes | const="gogurt-listener-status/v1" |  |
| <a id="s-4a920fa890"></a>`state_dir` | yes | type="string" |  |

##### <a id="s-b2e78b5b65"></a>field `listener_host_provider`


###### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| 1 | [See field `listener_host_provider` · `anyOf` alternative 1](#s-320c20e357) |
| <a id="s-d933870258"></a>2 | type="null" |

##### <a id="s-fc31c919f9"></a>field `mounted_volume_provider`


###### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| 1 | [See field `mounted_volume_provider` · `anyOf` alternative 1](#s-455aabb5a2) |
| <a id="s-86ab4a83ea"></a>2 | type="null" |

##### <a id="s-320c20e357"></a>field `listener_host_provider` · `anyOf` alternative 1

- <a id="s-a67f62825e"></a>`type`: `"object"`
- <a id="s-15a184c961"></a>`additionalProperties`: `false`
- <a id="s-71cf2a4707"></a>`required`: `["kind","name","provider_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bb60a74349"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-dffdf3814f"></a>`name` | yes | type="string" |  |
| <a id="s-a6750913a3"></a>`provider_id` | yes | type="string" |  |

##### <a id="s-455aabb5a2"></a>field `mounted_volume_provider` · `anyOf` alternative 1

- <a id="s-80fa41613f"></a>`type`: `"object"`
- <a id="s-5465d7f66c"></a>`additionalProperties`: `false`
- <a id="s-7a88739e56"></a>`required`: `["kind","name","provider_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a9a0d5401"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-04fe0eecf3"></a>`name` | yes | type="string" |  |
| <a id="s-56c5e59d3a"></a>`provider_id` | yes | type="string" |  |

#### <a id="s-7e6ce2c365"></a>`gogurt-cli-error/v1`

Applies to: operational · stdout (json).

<a id="s-9a72ce7d54"></a>

- <a id="s-10416fe8e4"></a>`type`: `"object"`
- <a id="s-6c8dbb853d"></a>`additionalProperties`: `false`
- <a id="s-a2e1437a14"></a>`required`: `["error"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | [See field `error`](#s-5cdf4abeb0) |  |

##### <a id="s-5cdf4abeb0"></a>field `error`

- <a id="s-bc7f91fae2"></a>`type`: `"object"`
- <a id="s-af45187999"></a>`additionalProperties`: `false`
- <a id="s-dc06486632"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6ba7996b85"></a>`code` | yes | enum=["config_error","listener_error"] |  |
| <a id="s-5da8235721"></a>`message` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-2e525796ff) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --listener-host-provider](#s-47e3cfdefa) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-af73aedac4"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-ad83f8541a"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources/authorities.md#src-3b2297c37d) — [reference/gogurt/application/src/gogurt/cli.py::&lt;module&gt;](../../../../../../reference/gogurt/application/src/gogurt/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/status/allow_extra_args`
- `/external_contract/cli/gogurt/commands/listener/commands/status/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/listener/commands/status/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/listener/commands/status/name`
- `/external_contract/cli/gogurt/commands/listener/commands/status/parameters`
- `/external_contract/cli/gogurt/commands/listener/commands/status/result_contract`
- `/external_contract/cli/gogurt/commands/listener/commands/status/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/listener/commands/status/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/listener/commands/status/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/listener/commands/status/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/listener/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/gogurt/commands/listener/commands/status/parameters`

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

### `/external_contract/cli/gogurt/commands/listener/commands/status/result_contract`

<!-- exact-contract-value: 90f736fa5bf8066acc17e2cc5647b8b2985d5ededab1787a1bfa8d1a1c1e4b65 -->

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
  "identity": "gogurt-cli-result/listener/status/v1",
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

### `/external_contract/cli/gogurt/commands/listener/commands/status/terminating_controls`

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
