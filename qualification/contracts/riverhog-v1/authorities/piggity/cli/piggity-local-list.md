# piggity local list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-list:1bdfba0680 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-dd9eeb09f0"></a>Parser name: `list`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-41841301f6"></a>`page_size` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'maximum': 100, 'minimum': 1, 'name': 'integer range'} | --page-size |
| <a id="s-460a35c4c7"></a>`page_token` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --page-token |
| <a id="s-35b952a3b8"></a>`sort` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --sort |
| <a id="s-6c9551143d"></a>`order` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --order |
| <a id="s-8e3801b0cd"></a>`query` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --query, -q |
| <a id="s-a030650e72"></a>`ids` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --ids |
| <a id="s-5c1d09f56e"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-b147615a14"></a>`help` | <a id="s-1fa68daec4"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-228b2ef204"></a>`0` | <a id="s-9a244eaaf5"></a>`"noncontractual-framework-help"` | <a id="s-07b06fba5a"></a>`"empty"` |

### Result and failure contract

- <a id="s-ea08a2c5f1"></a>Result identity: `piggity-cli-result/local/list/v1`
- <a id="s-e107637f6c"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-7fdfe9b921"></a>Structured output: `optional-json`
- <a id="s-424b4be15f"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b40ede78f3"></a>`completed` | <a id="s-a79553386e"></a>`{"kind":"command-completed"}` | <a id="s-e624bc94a7"></a>`0` | <a id="s-e8ebb0f401"></a>`human: noncontractual-presentation-of-command-result; json: piggity-local-collection-list/v1` | <a id="s-a173379636"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-53a4388e21"></a>`usage` | <a id="s-52e88b1a8f"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-00c8e54d75"></a>`2` | <a id="s-be36d857ec"></a>`all: empty` | <a id="s-c937018d8c"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-d3e9f15cc0"></a>`operational` | <a id="s-4f43a96878"></a>`{"kind":"application-error"}` | <a id="s-60cec51350"></a>`1` | <a id="s-8a82f1b3f2"></a>`human: empty; json: http-api-contracts.ErrorResponse` | <a id="s-d1d2a276e7"></a>`human: noncontractual-diagnostic; json: empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --ids](#s-a030650e72) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-5c1d09f56e) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --order](#s-6c9551143d) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-size](#s-41841301f6) | `value · cli-value · contract_max` | maximum=100; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-41841301f6) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-460a35c4c7) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --query](#s-8e3801b0cd) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --sort](#s-35b952a3b8) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Governing policies

- <a id="pa-be5399a8a8"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-5f74263f86"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/list/name`
- `/external_contract/cli/piggity/commands/local/commands/list/parameters`
- `/external_contract/cli/piggity/commands/local/commands/list/result_contract`
- `/external_contract/cli/piggity/commands/local/commands/list/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/piggity/commands/local/commands/list/parameters`

<!-- exact-contract-value: df74d4a5cbe5595e4014772751ab4406e0ed0b2f885ee9b99b70c15e365629f6 -->

```json
[
  {
    "count": false,
    "default": 25,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "page_size",
    "nargs": 1,
    "options": [
      "--page-size"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntRange",
      "maximum": 100,
      "minimum": 1,
      "name": "integer range"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "page_token",
    "nargs": 1,
    "options": [
      "--page-token"
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
    "default": "collection_id",
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "sort",
    "nargs": 1,
    "options": [
      "--sort"
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
    "default": "asc",
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "order",
    "nargs": 1,
    "options": [
      "--order"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "query",
    "nargs": 1,
    "options": [
      "--query",
      "-q"
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

### `/external_contract/cli/piggity/commands/local/commands/list/result_contract`

<!-- exact-contract-value: c729f928b1cdb8021508df95b649d657780301731d876e11787184a2974c60ca -->

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
  "identity": "piggity-cli-result/local/list/v1",
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
          "identity": "piggity-local-collection-list/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "collections": {
                "items": {
                  "additionalProperties": false,
                  "properties": {
                    "bytes": {
                      "minimum": 0,
                      "type": "integer"
                    },
                    "collection_id": {
                      "minimum": 1,
                      "type": "integer"
                    },
                    "created_at": {
                      "type": "string"
                    },
                    "files": {
                      "minimum": 0,
                      "type": "integer"
                    },
                    "status": {
                      "enum": [
                        "desired",
                        "remote-deleted",
                        "synchronizing"
                      ]
                    },
                    "tag_count": {
                      "minimum": 0,
                      "type": "integer"
                    }
                  },
                  "required": [
                    "collection_id",
                    "created_at",
                    "tag_count",
                    "status",
                    "files",
                    "bytes"
                  ],
                  "type": "object"
                },
                "type": "array"
              },
              "next_page_token": {
                "type": [
                  "string",
                  "null"
                ]
              },
              "order": {
                "enum": [
                  "asc",
                  "desc"
                ]
              },
              "page_size": {
                "maximum": 100,
                "minimum": 1,
                "type": "integer"
              },
              "query": {
                "type": [
                  "string",
                  "null"
                ]
              },
              "sort": {
                "enum": [
                  "bytes",
                  "collection_id",
                  "created_at",
                  "files",
                  "status"
                ]
              }
            },
            "required": [
              "page_size",
              "next_page_token",
              "sort",
              "order",
              "query",
              "collections"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/local/commands/list/terminating_controls`

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
