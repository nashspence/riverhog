# piggity collection provenance show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance-show:2e56f0359d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-e3cd0a43db"></a>Parser name: `show`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-264a1748ef"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-157533c3ca"></a>`path` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | path |
| <a id="s-118774fada"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-43b5eb78d9"></a>`help` | <a id="s-d3789afb22"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-08b599f85d"></a>`0` | <a id="s-6718447576"></a>`"noncontractual-framework-help"` | <a id="s-f07429935e"></a>`"empty"` |

### Result and failure contract

- <a id="s-b0c1e318a9"></a>Result identity: `piggity-cli-result/collection/provenance/show/v1`
- <a id="s-377b08a61c"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-40fae5b7d3"></a>Structured output: `optional-json`
- <a id="s-a025ffcaeb"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6f8c823624"></a>`completed` | <a id="s-ecc7ffcdcb"></a>`{"kind":"command-completed"}` | <a id="s-4df9534047"></a>`0` | <a id="s-ac7d9d9a4c"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP get_collection_file_provenance response 200](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-files-path.md#s-15bacadd6f) | <a id="s-e7a7c9c72b"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-84612799e9"></a>`usage` | <a id="s-030d76e2aa"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-7456b95d99"></a>`2` | <a id="s-14d81ff448"></a>all: `empty` | <a id="s-8b2b4ffb6e"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-b7af29cf0b"></a>`operational` | <a id="s-a662dcfcca"></a>`{"kind":"application-error"}` | <a id="s-c4935c6b8f"></a>`1` | <a id="s-eceb9ea4b7"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-bd2d9f5ed2"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-264a1748ef) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-118774fada) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter path](#s-157533c3ca) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/files/{path}](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-files-path.md)

## Governing policies

- <a id="pa-b824c38275"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-201e993079"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/parameters`

<!-- exact-contract-value: 5634b44414826da829550303b98e1286ca305c8d87b14ee3287aef4841c3c64d -->

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
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "path",
    "nargs": 1,
    "options": [
      "path"
    ],
    "required": true,
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

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/result_contract`

<!-- exact-contract-value: 6b659c9ee4786a05538198b16cb60cd38f762607be44e7983ae5f029aa68a068 -->

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
  "identity": "piggity-cli-result/collection/provenance/show/v1",
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
          "operation_id": "get_collection_file_provenance",
          "path": "/v1/collections/{collection_id}/provenance/files/{path}",
          "schema": {
            "$ref": "#/components/schemas/CollectionFileProvenanceDetailOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/terminating_controls`

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
