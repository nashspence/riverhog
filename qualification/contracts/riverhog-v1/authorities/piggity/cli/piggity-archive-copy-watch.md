# piggity archive copy watch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-archive-copy-watch:ed2be866af -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-887c110d92"></a>Parser name: `watch`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-078e3f681f"></a>`selector` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | selector |
| <a id="s-d555ad43d2"></a>`interval` | TyperOption | no | {'class': 'typer._click.types.FloatRange', 'minimum': 0.1, 'name': 'float range'} | --interval |
| <a id="s-d51d9a083a"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-12d8abb1ff"></a>`help` | <a id="s-7db4f12f8b"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-9677b957bf"></a>`0` | <a id="s-71f750492a"></a>`"noncontractual-framework-help"` | <a id="s-e6bff29fef"></a>`"empty"` |

### Result and failure contract

- <a id="s-3bb659ae93"></a>Result identity: `piggity-cli-result/archive/copy/watch/v1`
- <a id="s-bcad18689d"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-6af3e893d4"></a>Structured output: `optional-json`
- <a id="s-ed376ea7a6"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-ad41e45553"></a>`completed` | <a id="s-68d8597cf2"></a>`{"kind":"command-completed"}` | <a id="s-6778b37f32"></a>`0` | <a id="s-264091cc9f"></a>`human: noncontractual-presentation-of-command-result; json: HTTP get_archive_copy_job — #/components/schemas/ArchiveCopyJobOut` | <a id="s-6e0e616a5a"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5964ecb969"></a>`usage` | <a id="s-4dec2a6bde"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-8d34e74b97"></a>`2` | <a id="s-d7e4ddcaa7"></a>`all: empty` | <a id="s-d2a97b1ef8"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-a21748acac"></a>`operational` | <a id="s-d774e5049d"></a>`{"kind":"application-error"}` | <a id="s-9e31f9a4b3"></a>`1` | <a id="s-bb537a5b68"></a>`human: empty; json: http-api-contracts.ErrorResponse` | <a id="s-32dc682159"></a>`human: noncontractual-diagnostic; json: empty` |
| <a id="s-4272e15497"></a>`terminal-job-failure` | <a id="s-ffcfd47173"></a>`{"kind":"archive-copy-state","state":"failed"}` | <a id="s-6bde8974f5"></a>`1` | <a id="s-d2e75d2ac8"></a>`human: noncontractual-presentation-of-command-result; json: HTTP get_archive_copy_job — #/components/schemas/ArchiveCopyJobOut` | <a id="s-d5dd66039c"></a>`all: empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --interval](#s-d555ad43d2) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-d51d9a083a) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter selector](#s-078e3f681f) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/archive/copies/{collection_id}/{destination_store}](../../riverhog/http-operations/get-v1-archive-copies-collection-id-destination-store.md)

## Governing policies

- <a id="pa-bcfeeee4cd"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-bdf9e1dfc0"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/name`
- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/parameters`
- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/result_contract`
- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/name`

<!-- exact-contract-value: 73da76bff71a604995ddd94e223ffa8b7c171b54e0a953c0fb794ac85a61534b -->

```json
"watch"
```

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/parameters`

<!-- exact-contract-value: eb6e1b2a1271291e3a99a3c15afd19951a252010905621030980f9b269a6d741 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "selector",
    "nargs": 1,
    "options": [
      "selector"
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
    "default": 1,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "interval",
    "nargs": 1,
    "options": [
      "--interval"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.FloatRange",
      "minimum": 0.1,
      "name": "float range"
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

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/result_contract`

<!-- exact-contract-value: e25f7de161aa3eeb7a85f7d4628acb7911cf66cafa2f96ce0ff32638c2fa0c91 -->

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
    },
    {
      "exit_status": 1,
      "id": "terminal-job-failure",
      "selected_by": {
        "kind": "archive-copy-state",
        "state": "failed"
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
          "operation_id": "get_archive_copy_job",
          "path": "/v1/archive/copies/{collection_id}/{destination_store}",
          "schema": {
            "$ref": "#/components/schemas/ArchiveCopyJobOut"
          },
          "status": "200"
        }
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "piggity-cli-result/archive/copy/watch/v1",
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
          "operation_id": "get_archive_copy_job",
          "path": "/v1/archive/copies/{collection_id}/{destination_store}",
          "schema": {
            "$ref": "#/components/schemas/ArchiveCopyJobOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/terminating_controls`

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
