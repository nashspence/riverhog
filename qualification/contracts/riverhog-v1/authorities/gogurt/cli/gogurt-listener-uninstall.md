# gogurt listener uninstall

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-uninstall:3753ed9145 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-bf49e85607"></a>Parser name: `uninstall`

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
| <a id="s-3d05495670"></a>`completed` | <a id="s-6a33c82bf9"></a>`{"kind":"command-completed"}` | <a id="s-b5bb63891f"></a>`0` | <a id="s-f498ce6822"></a>human: `noncontractual-presentation-of-command-result`; json: [gogurt-listener-status/v1](#s-f498ce6822) | <a id="s-2752e77382"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a145130e57"></a>`usage` | <a id="s-9c6f31135e"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-1efb38269b"></a>`2` | <a id="s-c32269de3c"></a>all: `empty` | <a id="s-17bfb09a33"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-af71b8dcf5"></a>`operational` | <a id="s-24673a8203"></a>`{"kind":"application-error"}` | <a id="s-33bfc36d90"></a>`1` | <a id="s-5f08400698"></a>human: `empty`; json: [gogurt-cli-error/v1](#s-5f08400698) | <a id="s-2428b3eefe"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-9f84e8d6b1) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --listener-host-provider](#s-9bf6e52d46) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Governing policies

- <a id="pa-972f5bfe39"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-f68352ce46"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/uninstall/name`
- `/external_contract/cli/gogurt/commands/listener/commands/uninstall/parameters`
- `/external_contract/cli/gogurt/commands/listener/commands/uninstall/result_contract`
- `/external_contract/cli/gogurt/commands/listener/commands/uninstall/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

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
