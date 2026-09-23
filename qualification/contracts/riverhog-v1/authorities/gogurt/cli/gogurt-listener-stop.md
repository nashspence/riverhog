# gogurt listener stop

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-stop:7e1603429a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-b8a05038de"></a>Parser name: `stop`
- <a id="s-e0e8deff85"></a>Extra arguments at this parser: rejected.
- <a id="s-7b0bd0ce75"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-89c486f24a"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-8a6e9e8180"></a>`listener_host_provider`<br>`--listener-host-provider` | optional option; 1 value | text | not recorded<br>Env: `"GOGURT_LISTENER_HOST_PROVIDER"` |
| <a id="s-9964b5c031"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-4b5a394fb5"></a>`help` | <a id="s-6a55708d17"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-17b2d6c9d5"></a>`0` | <a id="s-12175d19cc"></a>`"noncontractual-framework-help"` | <a id="s-cfd22c77c0"></a>`"empty"` |

### Result and failure contract

- <a id="s-e75c2fc877"></a>Result identity: `gogurt-cli-result/listener/stop/v1`
- <a id="s-b0f82d156c"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-a21f964eb3"></a>Structured output: `optional-json`
- <a id="s-1329e8ef07"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-413bf5ade1"></a>`completed` | <a id="s-31c47a75a2"></a>`{"kind":"command-completed"}` | <a id="s-d7087fdf5d"></a>`0` | <a id="s-d10a50bdd8"></a>human: `"noncontractual-presentation-of-command-result"`; json: [gogurt-listener-status/v1](#s-8a3c97fd8b) | <a id="s-ed27c8de11"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-97035ade3f"></a>`usage` | <a id="s-d415ce5cf4"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-b719577371"></a>`2` | <a id="s-3e2f239655"></a>all: `"empty"` | <a id="s-0aeb4e772d"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-34ad9f46fd"></a>`operational` | <a id="s-b0b04eb16f"></a>`{"kind":"application-error"}` | <a id="s-6074c71542"></a>`1` | <a id="s-c5185a10e4"></a>human: `"empty"`; json: [gogurt-cli-error/v1](#s-8f4b7ca766) | <a id="s-8cdb0c18a5"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-8a3c97fd8b"></a>`gogurt-listener-status/v1`

Applies to: completed · stdout (json).

<a id="s-e76f47d6cb"></a>

- <a id="s-9ad1f9da7e"></a>`type`: `"object"`
- <a id="s-5677827007"></a>`additionalProperties`: `false`
- <a id="s-c0b9bd00a3"></a>`required`: `["schema","manager_version","platform","installed","enabled","running","health","config_file","state_dir","executable","mounted_volume_provider","listener_host_provider","heartbeat_age_seconds","heartbeat","dispatches","mount_attention","diagnostic"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4baa1a0b69"></a>`config_file` | yes | type="string" |  |
| <a id="s-4a66287501"></a>`diagnostic` | yes | type=["string","null"] |  |
| <a id="s-d28b7eb8f6"></a>`dispatches` | yes | type="object" |  |
| <a id="s-0fd4b353b6"></a>`enabled` | yes | type="boolean" |  |
| <a id="s-48f0c38df6"></a>`executable` | yes | type=["string","null"] |  |
| <a id="s-b89840ef3e"></a>`health` | yes | enum=["absent","stopped","failed","starting","healthy","stale"] |  |
| <a id="s-1644a51ba3"></a>`heartbeat` | yes | type=["object","null"] |  |
| <a id="s-fab6694148"></a>`heartbeat_age_seconds` | yes | type=["number","null"]; minimum=0 |  |
| <a id="s-ee455eeca6"></a>`installed` | yes | type="boolean" |  |
| `listener_host_provider` | yes | [See field `listener_host_provider`](#s-aedc58d84b) |  |
| <a id="s-c6c111c226"></a>`manager_version` | yes | type="string" |  |
| <a id="s-a2e99f5389"></a>`mount_attention` | yes | type="array" |  |
| `mounted_volume_provider` | yes | [See field `mounted_volume_provider`](#s-8e4fcebe46) |  |
| <a id="s-cafecbe18d"></a>`platform` | yes | type="string" |  |
| <a id="s-3a03d9b4eb"></a>`running` | yes | type="boolean" |  |
| <a id="s-0383182bd5"></a>`schema` | yes | const="gogurt-listener-status/v1" |  |
| <a id="s-1be6ca5932"></a>`state_dir` | yes | type="string" |  |

##### <a id="s-aedc58d84b"></a>field `listener_host_provider`


###### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| 1 | [See field `listener_host_provider` · `anyOf` alternative 1](#s-c6b44e3dcb) |
| <a id="s-5fc1c7b787"></a>2 | type="null" |

##### <a id="s-8e4fcebe46"></a>field `mounted_volume_provider`


###### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| 1 | [See field `mounted_volume_provider` · `anyOf` alternative 1](#s-cefb3d5c72) |
| <a id="s-08ebebd154"></a>2 | type="null" |

##### <a id="s-c6b44e3dcb"></a>field `listener_host_provider` · `anyOf` alternative 1

- <a id="s-1818f37519"></a>`type`: `"object"`
- <a id="s-d6e5011366"></a>`additionalProperties`: `false`
- <a id="s-a55dbbf422"></a>`required`: `["kind","name","provider_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-238cdd232b"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-68340476e6"></a>`name` | yes | type="string" |  |
| <a id="s-59db9edf38"></a>`provider_id` | yes | type="string" |  |

##### <a id="s-cefb3d5c72"></a>field `mounted_volume_provider` · `anyOf` alternative 1

- <a id="s-787fa3735d"></a>`type`: `"object"`
- <a id="s-fc972f6651"></a>`additionalProperties`: `false`
- <a id="s-47ae60fa18"></a>`required`: `["kind","name","provider_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-08a9679706"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-2b9d732107"></a>`name` | yes | type="string" |  |
| <a id="s-680cec24a2"></a>`provider_id` | yes | type="string" |  |

#### <a id="s-8f4b7ca766"></a>`gogurt-cli-error/v1`

Applies to: operational · stdout (json).

<a id="s-0faceebb62"></a>

- <a id="s-15f0b7c257"></a>`type`: `"object"`
- <a id="s-7ea96f6055"></a>`additionalProperties`: `false`
- <a id="s-6619a549d7"></a>`required`: `["error"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | [See field `error`](#s-6f279911fa) |  |

##### <a id="s-6f279911fa"></a>field `error`

- <a id="s-67b4c8558a"></a>`type`: `"object"`
- <a id="s-dff5f10a1f"></a>`additionalProperties`: `false`
- <a id="s-04a1a5f03a"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-638458b15e"></a>`code` | yes | enum=["config_error","listener_error"] |  |
| <a id="s-9a31b95e85"></a>`message` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-9964b5c031) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --listener-host-provider](#s-8a6e9e8180) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b44a04c0b2"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-5de456c976"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources/authorities.md#src-3b2297c37d) — [some-implementations/gogurt/application/src/gogurt/cli.py::&lt;module&gt;](../../../../../../some-implementations/gogurt/application/src/gogurt/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/stop/allow_extra_args`
- `/external_contract/cli/gogurt/commands/listener/commands/stop/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/listener/commands/stop/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/listener/commands/stop/name`
- `/external_contract/cli/gogurt/commands/listener/commands/stop/parameters`
- `/external_contract/cli/gogurt/commands/listener/commands/stop/result_contract`
- `/external_contract/cli/gogurt/commands/listener/commands/stop/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/listener/commands/stop/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/listener/commands/stop/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/listener/commands/stop/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/listener/commands/stop/name`

<!-- exact-contract-value: 6f2be2fe58ca2ba48e809c9588269fb710ce2535ba22acafb1123cdbb7421a02 -->

```json
"stop"
```

### `/external_contract/cli/gogurt/commands/listener/commands/stop/parameters`

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

### `/external_contract/cli/gogurt/commands/listener/commands/stop/result_contract`

<!-- exact-contract-value: fd4bdcd55b539778ccbd792cdb3e2abdd01eea7b07c03d495afa01db10fddd38 -->

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
  "identity": "gogurt-cli-result/listener/stop/v1",
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

### `/external_contract/cli/gogurt/commands/listener/commands/stop/terminating_controls`

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
