# piggity collection tag add

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-tag-add:316a39959c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-e5409890af"></a>Parser name: `add`

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-89f773ed42"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded |
| <a id="s-59325e9744"></a>`tag`<br>`tag` | required positional; 1 value | text | not recorded |
| <a id="s-487a06b9dc"></a>`revision`<br>`--revision` | optional option; 1 value | integer range; minimum=`1` | not recorded |
| <a id="s-a62b2b4357"></a>`tag_set_identity`<br>`--tag-set-identity` | optional option; 1 value | text | not recorded |
| <a id="s-5f64c2a2b6"></a>`operation_id`<br>`--operation-id` | optional option; 1 value | text | not recorded |
| <a id="s-0fed947d41"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-75c32ed75f"></a>`help` | <a id="s-f32c1ce563"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-8c29fda331"></a>`0` | <a id="s-7b41a9c9b5"></a>`"noncontractual-framework-help"` | <a id="s-0333ed3788"></a>`"empty"` |

### Result and failure contract

- <a id="s-e25fa0ddf2"></a>Result identity: `piggity-cli-result/collection/tag/add/v1`
- <a id="s-52746705af"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-e5556369a2"></a>Structured output: `optional-json`
- <a id="s-3858088b78"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-83dbedefc3"></a>`completed` | <a id="s-27cb297372"></a>`{"kind":"command-completed"}` | <a id="s-3d39e7d230"></a>`0` | <a id="s-a15682d4d9"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP add_collection_tag response 200](../../riverhog/http-operations/post-v1-collections-collection-id-tags-add.md#s-293ae74875) | <a id="s-799b1e002a"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0050a03f4e"></a>`usage` | <a id="s-6039fbbd5d"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-05aae3ab98"></a>`2` | <a id="s-8d69126a78"></a>all: `empty` | <a id="s-ac8393fcf1"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-b8e62ed087"></a>`operational` | <a id="s-df255f117c"></a>`{"kind":"application-error"}` | <a id="s-2a07c84dd5"></a>`1` | <a id="s-3644a24c46"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-cadd31fbbf"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-89f773ed42) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-0fed947d41) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --operation-id](#s-5f64c2a2b6) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --revision](#s-487a06b9dc) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter tag](#s-59325e9744) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --tag-set-identity](#s-a62b2b4357) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [POST /v1/collections/{collection_id}/tags:add](../../riverhog/http-operations/post-v1-collections-collection-id-tags-add.md)
- [riverhog_client.ApiClient.add_collection_tag](../../riverhog-client/python/riverhog-client-apiclient-add-collection-tag.md)
- [riverhog_client.ApiClient.get_collection](../../riverhog-client/python/riverhog-client-apiclient-get-collection.md)

## Governing policies

- <a id="pa-38488fb931"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-93af9e9168"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::collection_tag_add_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L1057)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/add/name`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/add/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/add/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/add/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/add/name`

<!-- exact-contract-value: 7b8a6f33b43ca26a3f2aa73e408748f9ceb391ac21dfe746c94563016ab72f85 -->

```json
"add"
```

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/add/parameters`

<!-- exact-contract-value: 1c409adfdd49b30b7c29eafa845578a90b1754270c23c112375011f77caeccb3 -->

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
    "name": "tag",
    "nargs": 1,
    "options": [
      "tag"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "revision",
    "nargs": 1,
    "options": [
      "--revision"
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
    "name": "tag_set_identity",
    "nargs": 1,
    "options": [
      "--tag-set-identity"
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
    "name": "operation_id",
    "nargs": 1,
    "options": [
      "--operation-id"
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

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/add/result_contract`

<!-- exact-contract-value: 73d21e727f63123079b11ca22f8f25cb177cc1e542bcc4b5724ea940517d4d1b -->

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
  "identity": "piggity-cli-result/collection/tag/add/v1",
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
          "method": "POST",
          "operation_id": "add_collection_tag",
          "path": "/v1/collections/{collection_id}/tags:add",
          "schema": {
            "$ref": "#/components/schemas/CollectionTagMutationOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/add/terminating_controls`

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
