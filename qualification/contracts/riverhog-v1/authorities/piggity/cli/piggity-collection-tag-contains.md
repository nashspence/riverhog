# piggity collection tag contains

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-tag-contains:789f3a7861 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-b569d0f93b"></a>Parser name: `contains`
- <a id="s-8a20b2daa5"></a>Extra arguments at this parser: rejected.
- <a id="s-f19489ac0f"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-46df7c4f7c"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-26476668d5"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-2482b9c520"></a>`tag`<br>`tag` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-4f6f468c54"></a>`revision`<br>`--revision` | optional option; 1 value | integer range; minimum=`1` (inclusive); outside range: reject | not recorded<br>Env: `null` |
| <a id="s-3380feb855"></a>`tag_set_identity`<br>`--tag-set-identity` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-dbfd84e078"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-ceaa0f7471"></a>`help` | <a id="s-47e0a37e2a"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-f4650cc940"></a>`0` | <a id="s-3e09c1a52f"></a>`"noncontractual-framework-help"` | <a id="s-23b2933cd3"></a>`"empty"` |

### Result and failure contract

- <a id="s-c17cb15bda"></a>Result identity: `piggity-cli-result/collection/tag/contains/v1`
- <a id="s-5d9a251515"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-aca710ae8f"></a>Structured output: `optional-json`
- <a id="s-d68d425b58"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-cb1e96f23c"></a>`completed` | <a id="s-e15078e80e"></a>`{"kind":"command-completed"}` | <a id="s-cce5a8053c"></a>`0` | <a id="s-642d4e9f5f"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP collection_contains_tag response 200](../../riverhog/http-operations/get-v1-collections-collection-id-tags-contains.md#s-02989a39c4) | <a id="s-addc816fa3"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5ce775ed08"></a>`usage` | <a id="s-b4495bb2a2"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-8ce329d0b4"></a>`2` | <a id="s-56f2161c4c"></a>all: `"empty"` | <a id="s-eacb26b0b9"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-af1778233a"></a>`operational` | <a id="s-bbd1de1f49"></a>`{"kind":"application-error"}` | <a id="s-92a6d9d9e1"></a>`1` | <a id="s-8c0219d819"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-17e7c8e235"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-26476668d5) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-dbfd84e078) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --revision](#s-4f6f468c54) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter tag](#s-2482b9c520) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --tag-set-identity](#s-3380feb855) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/tags:contains](../../riverhog/http-operations/get-v1-collections-collection-id-tags-contains.md)
- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [riverhog_client.ApiClient.collection_contains_tag](../../riverhog-client/python/riverhog-client-apiclient-collection-contains-tag.md)
- [riverhog_client.ApiClient.get_collection](../../riverhog-client/python/riverhog-client-apiclient-get-collection.md)

## Governing policies

- <a id="pa-16a606c9d1"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-4f27cf0bf1"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::collection\_tag\_contains\_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L1030)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/name`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/name`

<!-- exact-contract-value: dde0e3dc0ad9a0daf42c9d0ab5a9a1c466093b12ccae1ca4c613754a34526930 -->

```json
"contains"
```

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/parameters`

<!-- exact-contract-value: 545f7a270aa271b951b2b6ce4d0313284cd9758e6589f90cc348b4de22d965e2 -->

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
      "clamp": false,
      "class": "typer._click.types.IntRange",
      "max_open": false,
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

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/result_contract`

<!-- exact-contract-value: a2674cd3b3baba2c16f7aeed83f24bdcba37699c6f2940c2c1163ba9ed20f2cd -->

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
  "identity": "piggity-cli-result/collection/tag/contains/v1",
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
          "operation_id": "collection_contains_tag",
          "path": "/v1/collections/{collection_id}/tags:contains",
          "schema": {
            "$ref": "#/components/schemas/CollectionTagMembershipOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/terminating_controls`

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

</details>
