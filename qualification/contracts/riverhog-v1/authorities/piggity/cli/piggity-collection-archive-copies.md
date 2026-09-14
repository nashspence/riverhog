# piggity collection archive-copies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-archive-copies:a6f964a04a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-faf176b972"></a>Parser name: `archive-copies`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-fd2d1d91d9"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-63cae85eed"></a>`page_size` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'maximum': 100, 'minimum': 1, 'name': 'integer range'} | --page-size |
| <a id="s-6b36e57cf0"></a>`page_token` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --page-token |
| <a id="s-65f05156a0"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-698a8a054a"></a>`help` | <a id="s-27080df28c"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-a3a231bc0b"></a>`0` | <a id="s-195f599080"></a>`"noncontractual-framework-help"` | <a id="s-7912eeda73"></a>`"empty"` |

### Result and failure contract

- <a id="s-53de3ee4cd"></a>Result identity: `piggity-cli-result/collection/archive-copies/v1`
- <a id="s-9aa491e449"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-05958fa47a"></a>Structured output: `optional-json`
- <a id="s-c61a025b0f"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1301add70c"></a>`completed` | <a id="s-a3ce22a822"></a>`{"kind":"command-completed"}` | <a id="s-1f51bf48b7"></a>`0` | <a id="s-51e74a9852"></a>`human: noncontractual-presentation-of-command-result; json: HTTP list_collection_archive_copies — #/components/schemas/CollectionArchiveCopyListOut` | <a id="s-de5076a18a"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c71cdc5118"></a>`usage` | <a id="s-faa3d570d5"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-993411f574"></a>`2` | <a id="s-4d879d6944"></a>`all: empty` | <a id="s-439c7e0bb8"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-02774a4f41"></a>`operational` | <a id="s-fff05969fc"></a>`{"kind":"application-error"}` | <a id="s-69f907e682"></a>`1` | <a id="s-cc719f8aae"></a>`human: empty; json: http-api-contracts.ErrorResponse` | <a id="s-1ef89564e1"></a>`human: noncontractual-diagnostic; json: empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-fd2d1d91d9) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-65f05156a0) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-size](#s-63cae85eed) | `value · cli-value · contract_max` | maximum=100; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-63cae85eed) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-6b36e57cf0) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/archive-copies](../../riverhog/http-operations/get-v1-collections-collection-id-archive-copies.md)

## Governing policies

- <a id="pa-e8cf6d2cb8"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-1a1a7186ac"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/archive-copies/name`
- `/external_contract/cli/piggity/commands/collection/commands/archive-copies/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/archive-copies/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/archive-copies/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/archive-copies/name`

<!-- exact-contract-value: 9a7b14a8d6e94d549d6c335c673b29f17859372e5aa2328c9d495fba311b3335 -->

```json
"archive-copies"
```

### `/external_contract/cli/piggity/commands/collection/commands/archive-copies/parameters`

<!-- exact-contract-value: 4ef8b95a6ba091f67135744305ce0985d9ba04aca700d6ecefd7a4b1b64d6fe6 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "collection_id",
    "nargs": 1,
    "options": [
      "collection_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntParamType",
      "name": "integer"
    }
  },
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

### `/external_contract/cli/piggity/commands/collection/commands/archive-copies/result_contract`

<!-- exact-contract-value: 71e11ff8cc7a37d3162b0e4972a293ad7ebde974079fe6fcdf4818f525476b7d -->

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
  "identity": "piggity-cli-result/collection/archive-copies/v1",
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
          "application": "riverhog",
          "kind": "http-operation-response",
          "method": "GET",
          "operation_id": "list_collection_archive_copies",
          "path": "/v1/collections/{collection_id}/archive-copies",
          "schema": {
            "$ref": "#/components/schemas/CollectionArchiveCopyListOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/archive-copies/terminating_controls`

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
