# piggity retrieval cache show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-retrieval-cache-show:600adac62f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-03240d5f30"></a>Parser name: `show`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-8e6851d93a"></a>`selector` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | selector |
| <a id="s-7a43b24679"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c93ce30ac2"></a>`help` | <a id="s-01cb96fe8b"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-1f19d18c10"></a>`0` | <a id="s-fe2ada74ab"></a>`"noncontractual-framework-help"` | <a id="s-a225709769"></a>`"empty"` |

### Result and failure contract

- <a id="s-1a8516f693"></a>Result identity: `piggity-cli-result/retrieval/cache/show/v1`
- <a id="s-79afb23924"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-ca296406b6"></a>Structured output: `optional-json`
- <a id="s-e54913e840"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-8822d481ea"></a>`completed` | <a id="s-566e922e18"></a>`{"kind":"command-completed"}` | <a id="s-02040998ad"></a>`0` | <a id="s-2afc0f396d"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP get_retrieval_cache_object response 200](../../riverhog/http-operations/get-v1-retrieval-cache-objects-collection-id-source-store-object-id.md#s-ffc0a1dc71) | <a id="s-97a926ae57"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-cd0891737a"></a>`usage` | <a id="s-78dc07d2a1"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-9ed5168bf2"></a>`2` | <a id="s-59fa54c9c6"></a>all: `empty` | <a id="s-76e8fd70d5"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-a874738d53"></a>`operational` | <a id="s-9db75d79e6"></a>`{"kind":"application-error"}` | <a id="s-228e2b61e2"></a>`1` | <a id="s-cc7127df5b"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-6ab71e83d7"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-7a43b24679) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter selector](#s-8e6851d93a) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/retrieval-cache/objects/{collection_id}/{source_store}/{object_id}](../../riverhog/http-operations/get-v1-retrieval-cache-objects-collection-id-source-store-object-id.md)
- [riverhog_client.ApiClient.get_retrieval_cache_object](../../riverhog-client/python/riverhog-client-apiclient-get-retrieval-cache-object.md)

## Governing policies

- <a id="pa-b21974b866"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-2d09f93c23"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::retrieval_cache_show_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2850)

### Machine authority

- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/show/name`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/show/parameters`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/show/result_contract`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/show/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/show/parameters`

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

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/show/result_contract`

<!-- exact-contract-value: 4b776b372a593e625818a7022ec9f630c3af6c4396c58c94a7e6bbd3b85eb374 -->

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
  "identity": "piggity-cli-result/retrieval/cache/show/v1",
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
          "operation_id": "get_retrieval_cache_object",
          "path": "/v1/retrieval-cache/objects/{collection_id}/{source_store}/{object_id}",
          "schema": {
            "$ref": "#/components/schemas/RetrievalCacheObjectOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/show/terminating_controls`

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
