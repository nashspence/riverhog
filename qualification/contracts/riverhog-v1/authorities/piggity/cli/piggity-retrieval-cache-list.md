# piggity retrieval cache list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-retrieval-cache-list:0f179c1ed3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-da5e22501c"></a>Parser name: `list`

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-5d11091eaf"></a>`page_size`<br>`--page-size` | optional option; 1 value | integer range; minimum=`1`; maximum=`100` | `25` |
| <a id="s-c45391d32a"></a>`page_token`<br>`--page-token` | optional option; 1 value | text | not recorded |
| <a id="s-cf3cdc43ca"></a>`sort`<br>`--sort` | optional option; 1 value | text | `"cached_at"` |
| <a id="s-b5f96c6215"></a>`order`<br>`--order` | optional option; 1 value | text | `"desc"` |
| <a id="s-4b8d9c676b"></a>`query`<br>`--query`, `-q` | optional option; 1 value | text | not recorded |
| <a id="s-4093bf7690"></a>`collection_id`<br>`--collection` | optional option; 1 value | integer range; minimum=`1` | not recorded |
| <a id="s-4772990245"></a>`source_store`<br>`--source-store` | optional option; 1 value | text | not recorded |
| <a id="s-ed59a27577"></a>`cache_store`<br>`--cache-store` | optional option; 1 value | text | not recorded |
| <a id="s-b62947dae4"></a>`state`<br>`--state` | optional option; 1 value | text | not recorded |
| <a id="s-e8ba8122c4"></a>`protection`<br>`--protection` | optional option; 1 value | text | not recorded |
| <a id="s-a16c87a0c3"></a>`expires_before`<br>`--expires-before` | optional option; 1 value | text | not recorded |
| <a id="s-108befd372"></a>`expires_after`<br>`--expires-after` | optional option; 1 value | text | not recorded |
| <a id="s-4ccc692971"></a>`selectors`<br>`--selectors` | optional flag; 0 values | boolean | `false` |
| <a id="s-3e4c09ba42"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-25f6f450e7"></a>`help` | <a id="s-de01def640"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-45a8b9106e"></a>`0` | <a id="s-07e368b43b"></a>`"noncontractual-framework-help"` | <a id="s-45d7a0ca3b"></a>`"empty"` |

### Result and failure contract

- <a id="s-43402a0e46"></a>Result identity: `piggity-cli-result/retrieval/cache/list/v1`
- <a id="s-3d0c4710bf"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-9a1a4bce44"></a>Structured output: `optional-json`
- <a id="s-ba98e2ebd4"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-4d450e2673"></a>`completed` | <a id="s-342336639c"></a>`{"kind":"command-completed"}` | <a id="s-d7a0e6c1e2"></a>`0` | <a id="s-dfcd93e0b4"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP list_retrieval_cache_objects response 200](../../riverhog/http-operations/get-v1-retrieval-cache-objects.md#s-5f7e7e3a7a) | <a id="s-da8a898431"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0110a83f35"></a>`usage` | <a id="s-81ac3392ec"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-e49e283d15"></a>`2` | <a id="s-deda7c628a"></a>all: `empty` | <a id="s-2a29f1b471"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-a1d6144deb"></a>`operational` | <a id="s-30a291623e"></a>`{"kind":"application-error"}` | <a id="s-f31724d20f"></a>`1` | <a id="s-a3705b64a9"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-59137b7a09"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --cache-store](#s-ed59a27577) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --collection](#s-4093bf7690) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --expires-after](#s-108befd372) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --expires-before](#s-a16c87a0c3) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-3e4c09ba42) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --order](#s-b5f96c6215) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-size](#s-5d11091eaf) | `value · cli-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-5d11091eaf) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-c45391d32a) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --protection](#s-e8ba8122c4) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --query](#s-4b8d9c676b) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --selectors](#s-4ccc692971) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --sort](#s-cf3cdc43ca) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --source-store](#s-4772990245) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --state](#s-b62947dae4) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/retrieval-cache/objects](../../riverhog/http-operations/get-v1-retrieval-cache-objects.md)
- [riverhog_client.ApiClient.list_retrieval_cache_objects](../../riverhog-client/python/riverhog-client-apiclient-list-retrieval-cache-objects.md)

## Governing policies

- <a id="pa-8d4ee2b706"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-53df51e17f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::retrieval_cache_list_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2777)

### Machine authority

- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/list/name`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/list/parameters`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/list/result_contract`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/list/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/list/parameters`

<!-- exact-contract-value: c12bdb5d7cb6685bda1bf944422571c003f3bafabf9c219e16543381012694d5 -->

```json
[
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
    "default": "cached_at",
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "sort",
    "nargs": 1,
    "options": [
      "--sort"
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
    "default": "desc",
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "order",
    "nargs": 1,
    "options": [
      "--order"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "query",
    "nargs": 1,
    "options": [
      "--query",
      "-q"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "collection_id",
    "nargs": 1,
    "options": [
      "--collection"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntRange",
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
    "name": "source_store",
    "nargs": 1,
    "options": [
      "--source-store"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "cache_store",
    "nargs": 1,
    "options": [
      "--cache-store"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "state",
    "nargs": 1,
    "options": [
      "--state"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "protection",
    "nargs": 1,
    "options": [
      "--protection"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "expires_before",
    "nargs": 1,
    "options": [
      "--expires-before"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "expires_after",
    "nargs": 1,
    "options": [
      "--expires-after"
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
    "name": "selectors",
    "nargs": 1,
    "options": [
      "--selectors"
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

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/list/result_contract`

<!-- exact-contract-value: 2e514dea43144f4d61a1e48ecebf88de70189d269ae59bd0535983baa40bc5ce -->

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
  "identity": "piggity-cli-result/retrieval/cache/list/v1",
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
          "operation_id": "list_retrieval_cache_objects",
          "path": "/v1/retrieval-cache/objects",
          "schema": {
            "$ref": "#/components/schemas/RetrievalCacheObjectListOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/list/terminating_controls`

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
