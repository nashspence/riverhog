# gogurt listener status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-status:45a6e2afb0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-dea1d56be9"></a>Parser name: `status`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-47e3cfdefa"></a>`listener_host_provider` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --listener-host-provider |
| <a id="s-2e525796ff"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

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
| <a id="s-5a5305db12"></a>`completed` | <a id="s-32f5519ef9"></a>`{"kind":"command-completed"}` | <a id="s-89e35e5dbf"></a>`0` | <a id="s-be29598711"></a>human: `noncontractual-presentation-of-command-result`; json: [gogurt-listener-status/v1](#s-be29598711) | <a id="s-0c65c9fd90"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c1f67c0017"></a>`usage` | <a id="s-3aa709d1a0"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-4358d99190"></a>`2` | <a id="s-c5341bf6ae"></a>all: `empty` | <a id="s-db8f1f754c"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-6c2cb2d2f3"></a>`operational` | <a id="s-505cf6924b"></a>`{"kind":"application-error"}` | <a id="s-20b346ac94"></a>`1` | <a id="s-eb862390d6"></a>human: `empty`; json: [gogurt-cli-error/v1](#s-eb862390d6) | <a id="s-36936a865b"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-2e525796ff) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --listener-host-provider](#s-47e3cfdefa) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-5cb1a10f86"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-9e5f627e37"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/status/name`
- `/external_contract/cli/gogurt/commands/listener/commands/status/parameters`
- `/external_contract/cli/gogurt/commands/listener/commands/status/result_contract`
- `/external_contract/cli/gogurt/commands/listener/commands/status/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

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
