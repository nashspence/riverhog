# piggity local state verify

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-state-verify:7edf0b4146 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-342a39c4de"></a>Parser name: `verify`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-89fcb8302a"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-1350f1db36"></a>`help` | <a id="s-ae5ba19c67"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-45bd9cfd38"></a>`0` | <a id="s-76ac5ec1e2"></a>`"noncontractual-framework-help"` | <a id="s-39772de3db"></a>`"empty"` |

### Result and failure contract

- <a id="s-7aa9ce4b2c"></a>Result identity: `piggity-cli-result/local/state/verify/v1`
- <a id="s-0f47e93ccc"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-24c5cb446a"></a>Structured output: `optional-json`
- <a id="s-baf2662d14"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-824e1a5c46"></a>`completed` | <a id="s-69d27bfc2e"></a>`{"kind":"command-completed"}` | <a id="s-474687d39e"></a>`0` | <a id="s-a67428553f"></a>human: `noncontractual-presentation-of-command-result`; json: [state-schema-status/v1](#s-a67428553f) | <a id="s-2a6e1537eb"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-034f8e3ff5"></a>`usage` | <a id="s-b17302c70e"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-f790046202"></a>`2` | <a id="s-7935dbf686"></a>all: `empty` | <a id="s-1223d92c7e"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-4a7ba7f614"></a>`operational` | <a id="s-4161321eb1"></a>`{"kind":"application-error"}` | <a id="s-3b1830cdd0"></a>`1` | <a id="s-3ba9744dc4"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-bb01e98ae7"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-89fcb8302a) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-495fdf5a47"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-6a43b53fac"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/state/commands/verify/name`
- `/external_contract/cli/piggity/commands/local/commands/state/commands/verify/parameters`
- `/external_contract/cli/piggity/commands/local/commands/state/commands/verify/result_contract`
- `/external_contract/cli/piggity/commands/local/commands/state/commands/verify/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/state/commands/verify/name`

<!-- exact-contract-value: 898c74c2eed0452b1e51e567f237c37f1caa1e52f747466d56e76e15d07dc331 -->

```json
"verify"
```

### `/external_contract/cli/piggity/commands/local/commands/state/commands/verify/parameters`

<!-- exact-contract-value: f2cf9ed04ac608b58219dbcf22fc63be2fdf35901bc058f443df21b229aefd32 -->

```json
[
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

### `/external_contract/cli/piggity/commands/local/commands/state/commands/verify/result_contract`

<!-- exact-contract-value: 22d4a6db5caf62ad643faaaf2381d7972715465248c505083c66619bfce3907c -->

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
          "identity": "http-api-contracts.ErrorResponse",
          "kind": "python-model",
          "schema": {
            "$defs": {
              "ErrorBody": {
                "additionalProperties": false,
                "properties": {
                  "code": {
                    "minLength": 1,
                    "title": "Code",
                    "type": "string"
                  },
                  "details": {
                    "anyOf": [
                      {
                        "additionalProperties": true,
                        "type": "object"
                      },
                      {
                        "type": "null"
                      }
                    ],
                    "default": null,
                    "title": "Details"
                  },
                  "message": {
                    "minLength": 1,
                    "title": "Message",
                    "type": "string"
                  }
                },
                "required": [
                  "code",
                  "message"
                ],
                "title": "ErrorBody",
                "type": "object"
              }
            },
            "additionalProperties": false,
            "properties": {
              "error": {
                "$ref": "#/$defs/ErrorBody"
              }
            },
            "required": [
              "error"
            ],
            "title": "ErrorResponse",
            "type": "object"
          }
        }
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "piggity-cli-result/local/state/verify/v1",
  "profile_id": "piggity-cli-human-json/v1",
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
          "identity": "state-schema-status/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "condition": {
                "enum": [
                  "empty",
                  "current",
                  "upgrade_required",
                  "unversioned",
                  "incompatible"
                ]
              },
              "current_revision": {
                "type": [
                  "string",
                  "null"
                ]
              },
              "head_revision": {
                "type": "string"
              },
              "name": {
                "type": "string"
              }
            },
            "required": [
              "name",
              "condition",
              "current_revision",
              "head_revision"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/local/commands/state/commands/verify/terminating_controls`

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
