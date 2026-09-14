# piggity collection provenance trace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance-trace:ea6ba17b1b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-e59e3e8722"></a>Parser name: `trace`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-c091dc0d2a"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-7f88cb1d63"></a>`path` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | path |
| <a id="s-a86aaec58b"></a>`page_size` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'maximum': 100, 'minimum': 1, 'name': 'integer range'} | --page-size |
| <a id="s-731b929fc4"></a>`page_token` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --page-token |
| <a id="s-dd3e0b7e88"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-48348ef7d7"></a>`help` | <a id="s-6ddcc32b5b"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-18358c64c9"></a>`0` | <a id="s-9f2c111946"></a>`"noncontractual-framework-help"` | <a id="s-b9b09f9c06"></a>`"empty"` |

### Result and failure contract

- <a id="s-f8b89f66ec"></a>Result identity: `piggity-cli-result/collection/provenance/trace/v1`
- <a id="s-3fc06897ed"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-c89a2e57f7"></a>Structured output: `optional-json`
- <a id="s-1f0a591288"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-63bd56bc35"></a>`completed` | <a id="s-104c33a7c5"></a>`{"kind":"command-completed"}` | <a id="s-39e8e5dc3d"></a>`0` | <a id="s-05c47a8389"></a>`human: noncontractual-presentation-of-command-result; json: HTTP trace_collection_file_provenance — #/components/schemas/CollectionFileProvenanceTraceOut` | <a id="s-a8c5762a09"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-ae6b94dc3b"></a>`usage` | <a id="s-1731e3af7f"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-6c03461b49"></a>`2` | <a id="s-36ea324ac9"></a>`all: empty` | <a id="s-558e907617"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-72abffcff4"></a>`operational` | <a id="s-003d7854fe"></a>`{"kind":"application-error"}` | <a id="s-2a64c41928"></a>`1` | <a id="s-cf40e99a6b"></a>`human: empty; json: http-api-contracts.ErrorResponse` | <a id="s-7a71cc7d0d"></a>`human: noncontractual-diagnostic; json: empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-c091dc0d2a) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-dd3e0b7e88) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-size](#s-a86aaec58b) | `value · cli-value · contract_max` | maximum=100; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-a86aaec58b) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-731b929fc4) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter path](#s-7f88cb1d63) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/trace/{path}](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-trace-path.md)

## Governing policies

- <a id="pa-5a1f5d4902"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-89f536d5d9"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/name`

<!-- exact-contract-value: c9613997237f65491e06a014bcfff2df59b60534cf4c5d1eaf001e4764d4f15b -->

```json
"trace"
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/parameters`

<!-- exact-contract-value: 4dcf2fa5ffe51681fe8015d96788c1cf121b6a51576ca1e29553692ed7093ab6 -->

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

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/result_contract`

<!-- exact-contract-value: 9d79bd64c3c12ae6f7f98c55028ff17ea7803b6e45a14e6e55502fc78797b77a -->

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
  "identity": "piggity-cli-result/collection/provenance/trace/v1",
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
          "operation_id": "trace_collection_file_provenance",
          "path": "/v1/collections/{collection_id}/provenance/trace/{path}",
          "schema": {
            "$ref": "#/components/schemas/CollectionFileProvenanceTraceOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/terminating_controls`

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
