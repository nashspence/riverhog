# gogurt provider listener-host list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-provider-listener-host-list:eb86819ddb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-ac80695136"></a>Parser name: `list`

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-da38580369"></a>`ids`<br>`--ids` | optional flag; 0 values | boolean | `false` |
| <a id="s-7428604f93"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-0239e2c562"></a>`help` | <a id="s-eed54660ba"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-59aa202811"></a>`0` | <a id="s-4419a71184"></a>`"noncontractual-framework-help"` | <a id="s-aa1e1a071d"></a>`"empty"` |

### Result and failure contract

- <a id="s-9c1c9be4d0"></a>Result identity: `gogurt-cli-result/provider/listener-host/list/v1`
- <a id="s-20c7af8591"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-db4a6b0e64"></a>Structured output: `optional-json`
- <a id="s-31f9f2a643"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b7fcf63dc6"></a>`completed` | <a id="s-f6d51d477d"></a>`{"kind":"command-completed"}` | <a id="s-8258eab3ff"></a>`0` | <a id="s-f8c32bd2f5"></a>human: `noncontractual-presentation-of-command-result`; json: [gogurt-provider-list/v1](#s-f8c32bd2f5) | <a id="s-8fd7527f5a"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-afc40ae64b"></a>`usage` | <a id="s-6a13de0f39"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-2bafaab693"></a>`2` | <a id="s-05cfa13255"></a>all: `empty` | <a id="s-afa5cce429"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-4e487506f4"></a>`operational` | <a id="s-e6db970086"></a>`{"kind":"application-error"}` | <a id="s-126c99792b"></a>`1` | <a id="s-3ca0f36b10"></a>human: `empty`; json: [gogurt-cli-error/v1](#s-3ca0f36b10) | <a id="s-134483f0ec"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --ids](#s-da38580369) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-7428604f93) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-f919afe30d"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-9032b66006"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/name`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/parameters`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/result_contract`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/parameters`

<!-- exact-contract-value: e1d69a72acbf63fdc3459d233dcdf028761244a8bf1785d2a24bd76449d6da79 -->

```json
[
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "ids",
    "nargs": 1,
    "options": [
      "--ids"
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

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/result_contract`

<!-- exact-contract-value: 0013de87cd8ce9f11a9002d267438c36f01bb189bedfbe266e307745cf15cdec -->

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
  "identity": "gogurt-cli-result/provider/listener-host/list/v1",
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
          "identity": "gogurt-provider-list/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "format": {
                "const": "gogurt-provider-list/v1"
              },
              "kind": {
                "enum": [
                  "mounted-volume",
                  "listener-host"
                ]
              },
              "providers": {
                "items": {
                  "additionalProperties": false,
                  "properties": {
                    "distribution": {
                      "type": [
                        "string",
                        "null"
                      ]
                    },
                    "entry_point": {
                      "type": "string"
                    },
                    "kind": {
                      "enum": [
                        "mounted-volume",
                        "listener-host"
                      ]
                    },
                    "name": {
                      "type": "string"
                    },
                    "version": {
                      "type": [
                        "string",
                        "null"
                      ]
                    }
                  },
                  "required": [
                    "kind",
                    "name",
                    "entry_point",
                    "distribution",
                    "version"
                  ],
                  "type": "object"
                },
                "type": "array"
              }
            },
            "required": [
              "format",
              "kind",
              "providers"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/terminating_controls`

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
