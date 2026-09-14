# piggity archive copy cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-archive-copy-cancel:a25488b502 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-a126e99745"></a>Parser name: `cancel`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-a4a7803ca4"></a>`selector` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | selector |
| <a id="s-4dedd6e3c3"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-9a730a297d"></a>`help` | <a id="s-914605d54b"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-fd70d283e5"></a>`0` | <a id="s-9bc0ebc781"></a>`"noncontractual-framework-help"` | <a id="s-be8525a338"></a>`"empty"` |

### Result and failure contract

- <a id="s-10bf96cf1e"></a>Result identity: `piggity-cli-result/archive/copy/cancel/v1`
- <a id="s-457e4e09b6"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-d95ae52d22"></a>Structured output: `optional-json`
- <a id="s-b459ed6d9d"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-219c164e2e"></a>`completed` | <a id="s-19bdde062c"></a>`{"kind":"command-completed"}` | <a id="s-6509e27eca"></a>`0` | <a id="s-080b505264"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP cancel_archive_copy_job response 200](../../riverhog/http-operations/delete-v1-archive-copies-collection-id-destination-store.md#s-a26bbed67a) | <a id="s-cbf3bee2aa"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f147579986"></a>`usage` | <a id="s-7e6192791e"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-fb9a71625f"></a>`2` | <a id="s-4b3cbdea23"></a>all: `empty` | <a id="s-6e6154b385"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-44757edf01"></a>`operational` | <a id="s-c5eba1c2f5"></a>`{"kind":"application-error"}` | <a id="s-7082e8af4c"></a>`1` | <a id="s-93564acfe0"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-09ffc4b077"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-4dedd6e3c3) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter selector](#s-a4a7803ca4) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [DELETE /v1/archive/copies/{collection_id}/{destination_store}](../../riverhog/http-operations/delete-v1-archive-copies-collection-id-destination-store.md)

## Governing policies

- <a id="pa-741afe6b3d"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-b9757ebd91"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/cancel/name`
- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/cancel/parameters`
- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/cancel/result_contract`
- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/cancel/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/cancel/name`

<!-- exact-contract-value: 5a83111e44703c9dd7431bae2754317bb495994fbf736cf7739842ded4dcbc20 -->

```json
"cancel"
```

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/cancel/parameters`

<!-- exact-contract-value: 6b32bc56c11c2cdafb747d7a7aa740468c2f614b5b1d0e2588fb031d746e52f9 -->

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

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/cancel/result_contract`

<!-- exact-contract-value: a65ddc04d649084a2525d593a2880d393225812980639b10b8a4bca8b344527a -->

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
  "identity": "piggity-cli-result/archive/copy/cancel/v1",
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
          "method": "DELETE",
          "operation_id": "cancel_archive_copy_job",
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

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/cancel/terminating_controls`

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
