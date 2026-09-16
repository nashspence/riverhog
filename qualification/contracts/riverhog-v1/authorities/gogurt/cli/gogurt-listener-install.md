# gogurt listener install

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-install:392568a7e0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-50f526d032"></a>Parser name: `install`

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-a7098f7a64"></a>`config`<br>`--config` | optional option; 1 value | path | not recorded |
| <a id="s-e2444f543f"></a>`actions_dir`<br>`--actions-dir` | optional option; 1 value | path | not recorded |
| <a id="s-93b1bdc7db"></a>`interval_seconds`<br>`--interval` | optional option; 1 value | float range; minimum=`0.1`; maximum=`3600` | `2` |
| <a id="s-22d9cfd874"></a>`autorun`<br>`--autorun` | optional flag; 0 values | boolean | `false` |
| <a id="s-84c6a2811e"></a>`mounted_volume_provider`<br>`--mounted-volume-provider` | optional option; 1 value | text | not recorded<br>Env: `"GOGURT_MOUNTED_VOLUME_PROVIDER"` |
| <a id="s-0f93a46b54"></a>`listener_host_provider`<br>`--listener-host-provider` | optional option; 1 value | text | not recorded<br>Env: `"GOGURT_LISTENER_HOST_PROVIDER"` |
| <a id="s-e4798ae920"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-3af508d89b"></a>`help` | <a id="s-1b9a43b234"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-04a5125b53"></a>`0` | <a id="s-ba3be3e1c5"></a>`"noncontractual-framework-help"` | <a id="s-8c6d37bc18"></a>`"empty"` |

### Result and failure contract

- <a id="s-5dc6f486d4"></a>Result identity: `gogurt-cli-result/listener/install/v1`
- <a id="s-7b2614c5be"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-e2d4d5e3ba"></a>Structured output: `optional-json`
- <a id="s-d2f5dcad07"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-766debc0c6"></a>`completed` | <a id="s-924ff1ec78"></a>`{"kind":"command-completed"}` | <a id="s-b78a89ef1d"></a>`0` | <a id="s-81469e1a8d"></a>human: `noncontractual-presentation-of-command-result`; json: [gogurt-listener-status/v1](#s-81469e1a8d) | <a id="s-38b581ec69"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-7b2d01af13"></a>`usage` | <a id="s-fdb2015cfb"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-da5397f723"></a>`2` | <a id="s-bc93e447d5"></a>all: `empty` | <a id="s-dcd7042f84"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-7c484ca96c"></a>`operational` | <a id="s-6362e72f68"></a>`{"kind":"application-error"}` | <a id="s-b32ce984f1"></a>`1` | <a id="s-b355e6ba1c"></a>human: `empty`; json: [gogurt-cli-error/v1](#s-b355e6ba1c) | <a id="s-cf5f7725af"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --actions-dir](#s-e2444f543f) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --autorun](#s-22d9cfd874) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --config](#s-a7098f7a64) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --interval](#s-93b1bdc7db) | `value · cli-value · contract_max` | maximum=3600; minimum=0.1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --interval](#s-93b1bdc7db) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-e4798ae920) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --listener-host-provider](#s-0f93a46b54) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --mounted-volume-provider](#s-84c6a2811e) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Governing policies

- <a id="pa-fd2a70eb63"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-cfbbfe3391"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/install/name`
- `/external_contract/cli/gogurt/commands/listener/commands/install/parameters`
- `/external_contract/cli/gogurt/commands/listener/commands/install/result_contract`
- `/external_contract/cli/gogurt/commands/listener/commands/install/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/listener/commands/install/name`

<!-- exact-contract-value: 39975c7e71548c5154f38576f51bc8af358442cdc268413cd685d53cef71e9a1 -->

```json
"install"
```

### `/external_contract/cli/gogurt/commands/listener/commands/install/parameters`

<!-- exact-contract-value: a14d7546255da1a419ae8505f7913be301582bf048cb2a960b02bbc962c9e486 -->

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
      "class": "typer._click.types.FloatRange",
      "maximum": 3600,
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
  },
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

### `/external_contract/cli/gogurt/commands/listener/commands/install/result_contract`

<!-- exact-contract-value: 4e34ac269ce5889a02f4fa34b3f882f41621b2f7419a4cc73aa0e578182fddb9 -->

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
  "identity": "gogurt-cli-result/listener/install/v1",
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

### `/external_contract/cli/gogurt/commands/listener/commands/install/terminating_controls`

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
