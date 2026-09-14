# piggity collection describe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-describe:0f262e7209 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-2dd51f2dda"></a>Parser name: `describe`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-a79c8f4d1c"></a>`collection` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection |
| <a id="s-c6fc67bf05"></a>`description` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --description |
| <a id="s-6146b8bc01"></a>`clear` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --clear |
| <a id="s-fe5337d00f"></a>`if_match` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --if-match |
| <a id="s-92d36de323"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-b1a7bfa56f"></a>`help` | <a id="s-57da332599"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-e5a1a90e8a"></a>`0` | <a id="s-f6e2f4606f"></a>`"noncontractual-framework-help"` | <a id="s-c3bc7827bd"></a>`"empty"` |

### Result and failure contract

- <a id="s-73507093b9"></a>Result identity: `piggity-cli-result/collection/describe/v1`
- <a id="s-0017d54374"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-6db063e197"></a>Structured output: `optional-json`
- <a id="s-ae630e36a3"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-8a9d35f61d"></a>`completed` | <a id="s-1513ec233d"></a>`{"kind":"command-completed"}` | <a id="s-8b7a9a3b5a"></a>`0` | <a id="s-efb4b82fe7"></a>`human: noncontractual-presentation-of-command-result; json: HTTP replace_collection_description — #/components/schemas/CollectionDescriptionOut` | <a id="s-26df4d777f"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-9633bce9bc"></a>`usage` | <a id="s-03dccb19ab"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-da0edf292e"></a>`2` | <a id="s-8a528d9508"></a>`all: empty` | <a id="s-a1102f7496"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-802745aa94"></a>`operational` | <a id="s-13de4c4448"></a>`{"kind":"application-error"}` | <a id="s-aa10a762c2"></a>`1` | <a id="s-07a9c73ae3"></a>`human: empty; json: http-api-contracts.ErrorResponse` | <a id="s-2ad6252e1a"></a>`human: noncontractual-diagnostic; json: empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --clear](#s-6146b8bc01) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter collection](#s-a79c8f4d1c) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --description](#s-c6fc67bf05) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --if-match](#s-fe5337d00f) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-92d36de323) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [PUT /v1/collections/{collection_id}/description](../../riverhog/http-operations/put-v1-collections-collection-id-description.md)

## Governing policies

- <a id="pa-6adc3e2e41"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-5059334b03"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/describe/name`
- `/external_contract/cli/piggity/commands/collection/commands/describe/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/describe/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/describe/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/describe/name`

<!-- exact-contract-value: ccbb85a554fc61cc780e2cae6cc0e75e15a01539011884b8e460657a860ded8e -->

```json
"describe"
```

### `/external_contract/cli/piggity/commands/collection/commands/describe/parameters`

<!-- exact-contract-value: 645f658239e15189970ff762b7508b23e08fa999995a7f8ecbed88e0376a7925 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "collection",
    "nargs": 1,
    "options": [
      "collection"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "description",
    "nargs": 1,
    "options": [
      "--description"
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
    "name": "clear",
    "nargs": 1,
    "options": [
      "--clear"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "if_match",
    "nargs": 1,
    "options": [
      "--if-match"
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

### `/external_contract/cli/piggity/commands/collection/commands/describe/result_contract`

<!-- exact-contract-value: aee6e6212028db85a1538ca9db8955260b28dd21e31ef6d5696cbbf26a0025b9 -->

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
  "identity": "piggity-cli-result/collection/describe/v1",
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
          "method": "PUT",
          "operation_id": "replace_collection_description",
          "path": "/v1/collections/{collection_id}/description",
          "schema": {
            "$ref": "#/components/schemas/CollectionDescriptionOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/describe/terminating_controls`

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
