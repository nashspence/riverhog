# piggity collection upload watch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-upload-watch:760e32fe66 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-3f50299b41"></a>Parser name: `watch`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-2d6558a87d"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-514e76b029"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-450a2d916c"></a>`help` | <a id="s-6153b57f0a"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-8606ee63fd"></a>`0` | <a id="s-d04e13a3d5"></a>`"noncontractual-framework-help"` | <a id="s-53dfa20742"></a>`"empty"` |

### Result and failure contract

- <a id="s-41db34eacc"></a>Result identity: `piggity-cli-result/collection/upload/watch/v1`
- <a id="s-9a0709aeae"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-fee7be7e1b"></a>Structured output: `optional-json`
- <a id="s-9e22d744f0"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-acd3c70be4"></a>`completed` | <a id="s-a525d3475d"></a>`{"kind":"command-completed"}` | <a id="s-8272705a42"></a>`0` | <a id="s-6d0344c8cd"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP get_collection_upload_session response 200](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id.md#s-3478a57d08) | <a id="s-dea94d91e8"></a>all: `noncontractual-progress` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-ed4f6ab955"></a>`usage` | <a id="s-a6869ae2a2"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-cfd35862a8"></a>`2` | <a id="s-ce76b53ad7"></a>all: `empty` | <a id="s-d08182b49a"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-aa66567ab9"></a>`operational` | <a id="s-71e8b5506d"></a>`{"kind":"application-error"}` | <a id="s-600ca67dc8"></a>`1` | <a id="s-961fb3d8e1"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-f3cdf852d2"></a>human: `noncontractual-diagnostic-or-progress`; json: `noncontractual-progress` |
| <a id="s-5de1ee9645"></a>`custody-timeout` | <a id="s-8fbb430c42"></a>`{"kind":"custody-deadline-expired","state":"not-finalized"}` | <a id="s-7e83c1f628"></a>`124` | <a id="s-44154f6b2e"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP get_collection_upload_session response 200](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id.md#s-3478a57d08) | <a id="s-fbe773a64f"></a>all: `noncontractual-progress` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-2d6558a87d) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-514e76b029) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id.md)

## Governing policies

- <a id="pa-921cd95e68"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-0b7e6ecc36"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/watch/name`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/watch/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/watch/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/watch/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/watch/name`

<!-- exact-contract-value: 73da76bff71a604995ddd94e223ffa8b7c171b54e0a953c0fb794ac85a61534b -->

```json
"watch"
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/watch/parameters`

<!-- exact-contract-value: 69121b7dd4df39852c314f302ca34fb358e3d4472f9e5565bdec50242e30ee3a -->

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

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/watch/result_contract`

<!-- exact-contract-value: 611bc3b9701f36e710cb6c6b715df60587c5ce1f1c08a0102fda09c07d5a8994 -->

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
        "human": "noncontractual-diagnostic-or-progress",
        "json": "noncontractual-progress"
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
      "exit_status": 124,
      "id": "custody-timeout",
      "selected_by": {
        "kind": "custody-deadline-expired",
        "state": "not-finalized"
      },
      "stderr": {
        "all": "noncontractual-progress"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "application": "riverhog",
          "kind": "http-operation-response",
          "method": "GET",
          "operation_id": "get_collection_upload_session",
          "path": "/v1/collection-upload-sessions/{collection_id}",
          "schema": {
            "$ref": "#/components/schemas/CollectionUploadSessionOut"
          },
          "status": "200"
        }
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "piggity-cli-result/collection/upload/watch/v1",
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
        "all": "noncontractual-progress"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "application": "riverhog",
          "kind": "http-operation-response",
          "method": "GET",
          "operation_id": "get_collection_upload_session",
          "path": "/v1/collection-upload-sessions/{collection_id}",
          "schema": {
            "$ref": "#/components/schemas/CollectionUploadSessionOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/watch/terminating_controls`

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
