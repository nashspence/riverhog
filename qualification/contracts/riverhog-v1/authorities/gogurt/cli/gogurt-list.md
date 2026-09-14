# gogurt list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-list:094b3f0991 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-df82c9962d"></a>Parser name: `list`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-64caa42d6e"></a>`config` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'path'} | --config |
| <a id="s-52df517a50"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

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
| <a id="s-c3a2f34fe6"></a>`completed` | <a id="s-9be9b36324"></a>`{"kind":"command-completed"}` | <a id="s-9b9daa79b4"></a>`0` | <a id="s-80284ed44b"></a>`human: noncontractual-presentation-of-command-result; json: gogurt-route-list/v1` | <a id="s-51c0033cdf"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0dfa26dacd"></a>`usage` | <a id="s-170d4633cf"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-486ef488cb"></a>`2` | <a id="s-ebbf4ae230"></a>`all: empty` | <a id="s-65dd649f1a"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-9f9c616a50"></a>`operational` | <a id="s-4fe71ed413"></a>`{"kind":"application-error"}` | <a id="s-7b85f4007b"></a>`1` | <a id="s-9130a5c532"></a>`human: empty; json: gogurt-cli-error/v1` | <a id="s-a16876af5c"></a>`human: noncontractual-diagnostic; json: empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --config](#s-64caa42d6e) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-52df517a50) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-b7bd1c9e8f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-514af86d21"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/list/name`
- `/external_contract/cli/gogurt/commands/list/parameters`
- `/external_contract/cli/gogurt/commands/list/result_contract`
- `/external_contract/cli/gogurt/commands/list/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/gogurt/commands/list/parameters`

<!-- exact-contract-value: b0c1dccf67308fb5c7cd018f676b02ffe94f17f1b2aee1604942097a29f03aa0 -->

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
