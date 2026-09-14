# piggity local provenance-observer list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-provenance-observer-list:29149de089 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-8a8f779175"></a>Parser name: `list`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-1f4404fd32"></a>`ids` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --ids |
| <a id="s-a33a21e905"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-83c44de4ae"></a>`help` | <a id="s-d6340ed2a6"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-83f2afacef"></a>`0` | <a id="s-219a98674d"></a>`"noncontractual-framework-help"` | <a id="s-0190d55a9d"></a>`"empty"` |

### Result and failure contract

- <a id="s-ca87352466"></a>Result identity: `piggity-cli-result/local/provenance-observer/list/v1`
- <a id="s-3455802506"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-bf1f973c0a"></a>Structured output: `optional-json`
- <a id="s-3360c1f60c"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-4d07387287"></a>`completed` | <a id="s-566a58311b"></a>`{"kind":"command-completed"}` | <a id="s-d0dcda5f7f"></a>`0` | <a id="s-5cfdf77121"></a>`human: noncontractual-presentation-of-command-result; json: riverhog-provenance-observer-provider-list/v1` | <a id="s-0a9752937a"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-ae0364b0be"></a>`usage` | <a id="s-0abcda50e3"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-2ccb977fb1"></a>`2` | <a id="s-c7ec820b01"></a>`all: empty` | <a id="s-b04a1bfbd5"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-ef9729a98e"></a>`operational` | <a id="s-4ed4f8d5ab"></a>`{"kind":"application-error"}` | <a id="s-c78945c76a"></a>`1` | <a id="s-e553251cd3"></a>`human: empty; json: http-api-contracts.ErrorResponse` | <a id="s-6e928d10a6"></a>`human: noncontractual-diagnostic; json: empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --ids](#s-1f4404fd32) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-a33a21e905) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-5a6006f588"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-fde8389c97"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/list/name`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/list/parameters`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/list/result_contract`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/list/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/list/parameters`

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

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/list/result_contract`

<!-- exact-contract-value: 6bb04a3ce3749affa08ab2e6738c715c817f222961b4ac0c8fa687cd682fa31a -->

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
  "identity": "piggity-cli-result/local/provenance-observer/list/v1",
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
          "identity": "riverhog-provenance-observer-provider-list/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "format": {
                "const": "riverhog-provenance-observer-provider-list/v1"
              },
              "providers": {
                "items": {
                  "type": "object"
                },
                "type": "array"
              }
            },
            "required": [
              "format",
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

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/list/terminating_controls`

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
