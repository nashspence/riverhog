# piggity event list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-event-list:9a82aabe26 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d2138ca7d1"></a>Parser name: `list`

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-9b6e165302"></a>`after`<br>`--after` | optional option; 1 value | text | not recorded |
| <a id="s-694707cea7"></a>`limit`<br>`--limit` | optional option; 1 value | integer range; minimum=`1`; maximum=`100` | `100` |
| <a id="s-c9e9de4469"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-773c18a1be"></a>`help` | <a id="s-75a279d279"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-10b30a87a5"></a>`0` | <a id="s-5706a9ee9e"></a>`"noncontractual-framework-help"` | <a id="s-c642baa58d"></a>`"empty"` |

### Result and failure contract

- <a id="s-1728f46883"></a>Result identity: `piggity-cli-result/event/list/v1`
- <a id="s-b2f4885ef9"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-58b441cc6f"></a>Structured output: `optional-json`
- <a id="s-3adfe3feaa"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-d4b81f1d67"></a>`completed` | <a id="s-cc71c1afb1"></a>`{"kind":"command-completed"}` | <a id="s-1e5009a042"></a>`0` | <a id="s-ba9abb7899"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP list_lifecycle_events response 200](../../riverhog/http-operations/get-v1-events.md#s-61a7a9f60f) | <a id="s-2d67cc4fdd"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-97e53a9a49"></a>`usage` | <a id="s-4165c71793"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-d4065daacb"></a>`2` | <a id="s-8c8b8ee1cc"></a>all: `empty` | <a id="s-707a0236b9"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-5f4c97ef12"></a>`operational` | <a id="s-48a3416c4c"></a>`{"kind":"application-error"}` | <a id="s-aef0692b8e"></a>`1` | <a id="s-fd0c9e7767"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-1a3d29aec2"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --after](#s-9b6e165302) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-c9e9de4469) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --limit](#s-694707cea7) | `value · cli-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --limit](#s-694707cea7) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/events](../../riverhog/http-operations/get-v1-events.md)
- [riverhog_client.ApiClient.list_lifecycle_events](../../riverhog-client/python/riverhog-client-apiclient-list-lifecycle-events.md)

## Governing policies

- <a id="pa-8549d6343a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-b0fa7e0802"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::event_list_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L751)

### Machine authority

- `/external_contract/cli/piggity/commands/event/commands/list/name`
- `/external_contract/cli/piggity/commands/event/commands/list/parameters`
- `/external_contract/cli/piggity/commands/event/commands/list/result_contract`
- `/external_contract/cli/piggity/commands/event/commands/list/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/event/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/piggity/commands/event/commands/list/parameters`

<!-- exact-contract-value: d1dcd264e4d0682fb677304f77f84a4be86a712b177dc7e1262e2177ea8f6694 -->

```json
[
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "after",
    "nargs": 1,
    "options": [
      "--after"
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
    "default": 100,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "limit",
    "nargs": 1,
    "options": [
      "--limit"
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

### `/external_contract/cli/piggity/commands/event/commands/list/result_contract`

<!-- exact-contract-value: 8d86afe5bd79f3374ca3c32fcaf487a146843a15ba4fd904043a6fd705623b56 -->

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
  "identity": "piggity-cli-result/event/list/v1",
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
          "operation_id": "list_lifecycle_events",
          "path": "/v1/events",
          "schema": {
            "$ref": "#/components/schemas/RiverhogEventPage"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/event/commands/list/terminating_controls`

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
