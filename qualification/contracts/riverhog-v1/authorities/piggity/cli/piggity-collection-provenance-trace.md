# piggity collection provenance trace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance-trace:a33c984ba5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-e59e3e8722"></a>Parser name: `trace`
- <a id="s-ef18b22aee"></a>Extra arguments at this parser: rejected.
- <a id="s-5e47df438e"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-7ef1c39c75"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-c091dc0d2a"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded |
| <a id="s-7f88cb1d63"></a>`path`<br>`path` | required positional; 1 value | text | not recorded |
| <a id="s-a86aaec58b"></a>`page_size`<br>`--page-size` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `25` |
| <a id="s-731b929fc4"></a>`page_token`<br>`--page-token` | optional option; 1 value | text | not recorded |
| <a id="s-dd3e0b7e88"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

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
| <a id="s-63bd56bc35"></a>`completed` | <a id="s-104c33a7c5"></a>`{"kind":"command-completed"}` | <a id="s-39e8e5dc3d"></a>`0` | <a id="s-05c47a8389"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP trace_collection_file_provenance response 200](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-trace-path.md#s-88d1d82fa6) | <a id="s-a8c5762a09"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-ae6b94dc3b"></a>`usage` | <a id="s-1731e3af7f"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-6c03461b49"></a>`2` | <a id="s-36ea324ac9"></a>all: `empty` | <a id="s-558e907617"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-72abffcff4"></a>`operational` | <a id="s-003d7854fe"></a>`{"kind":"application-error"}` | <a id="s-2a64c41928"></a>`1` | <a id="s-cf40e99a6b"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-7a71cc7d0d"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-c091dc0d2a) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-dd3e0b7e88) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --page-size](#s-a86aaec58b) | `value · cli-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-a86aaec58b) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-731b929fc4) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter path](#s-7f88cb1d63) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/trace/{path}](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-trace-path.md)
- [riverhog_client.ApiClient.trace_collection_file_provenance](../../riverhog-client/python/riverhog-client-apiclient-trace-collection-file-provenance.md)

## Governing policies

- <a id="pa-39d373d3ef"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-bc9afce0cb"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::provenance_trace_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2635)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/name`

<!-- exact-contract-value: c9613997237f65491e06a014bcfff2df59b60534cf4c5d1eaf001e4764d4f15b -->

```json
"trace"
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/trace/parameters`

<!-- exact-contract-value: f80514bca7709ad11bbecc3a709c60d3c98d3ead69e18703748ee262f5d3f695 -->

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
      "clamp": false,
      "class": "typer._click.types.IntRange",
      "max_open": false,
      "maximum": 100,
      "min_open": false,
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
