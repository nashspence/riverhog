# a-riverhog-cli collection tag remove

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-tag-remove:eb40cf26f7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d366c46d15"></a>Parser name: `remove`
- <a id="s-c6f9676145"></a>Extra arguments at this parser: rejected.
- <a id="s-aca19ffa66"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-82c5fcd436"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-31eef78f15"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-d31df039a3"></a>`tag`<br>`tag` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-b5ecf9d3d4"></a>`revision`<br>`--revision` | optional option; 1 value | integer range; minimum=`1` (inclusive); outside range: reject | not recorded<br>Env: `null` |
| <a id="s-ee520965df"></a>`tag_set_identity`<br>`--tag-set-identity` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-5ca80018c9"></a>`operation_id`<br>`--operation-id` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-b63205fccb"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-8c94e51e4b"></a>`help` | <a id="s-63941d0b5b"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-1ea2264bda"></a>`0` | <a id="s-bff79a2366"></a>`"noncontractual-framework-help"` | <a id="s-47651e77da"></a>`"empty"` |

### Result and failure contract

- <a id="s-dc789d62c2"></a>Result identity: `a-riverhog-cli-result/collection/tag/remove/v1`
- <a id="s-2f37c1fbc6"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-2322e3ec95"></a>Structured output: `optional-json`
- <a id="s-1dac0ebe7a"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f5a10cfca0"></a>`completed` | <a id="s-5d3de85108"></a>`{"kind":"command-completed"}` | <a id="s-c58611c341"></a>`0` | <a id="s-709cd91255"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP remove_collection_tag response 200](../../riverhog/http-operations/post-v1-collections-collection-id-tags-remove.md#s-ea545b124d) | <a id="s-3f90c38e26"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-23cab349df"></a>`usage` | <a id="s-ad6ce6d5d7"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-897b3c107a"></a>`2` | <a id="s-3313d0d309"></a>all: `"empty"` | <a id="s-de339eae34"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-a9f96e5a1a"></a>`operational` | <a id="s-f70c896e69"></a>`{"kind":"application-error"}` | <a id="s-fbcc5c570b"></a>`1` | <a id="s-2581a96611"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-843cdc9b79"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-31eef78f15) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-b63205fccb) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --operation-id](#s-5ca80018c9) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --revision](#s-b5ecf9d3d4) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter tag](#s-d31df039a3) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --tag-set-identity](#s-ee520965df) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [POST /v1/collections/{collection_id}/tags:remove](../../riverhog/http-operations/post-v1-collections-collection-id-tags-remove.md)
- [riverhog_client.ApiClient.get_collection](../../riverhog-client/python/riverhog-client-apiclient-get-collection.md)
- [riverhog_client.ApiClient.remove_collection_tag](../../riverhog-client/python/riverhog-client-apiclient-remove-collection-tag.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c132991a55"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-843e8c2b30"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::collection\_tag\_remove\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L1088)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/remove/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/remove/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/remove/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/remove/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/remove/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/remove/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/remove/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/remove/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/remove/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/remove/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/remove/name`

<!-- exact-contract-value: 66f3d68b7627c9f6965029d01923daefa869b3f33123c145ddb25464b41a529a -->

```json
"remove"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/remove/parameters`

<!-- exact-contract-value: 23024ad83c5fd41b7ac0b5f06d52336cf0e25f1fdef5696c32f25fa253c71943 -->

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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/remove/result_contract`

<!-- exact-contract-value: 9ab7004325c5e9591a792e211c493fce8c401e916ccb90baef08a22bda814fdd -->

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
          "identity": "http-api-contracts.ErrorOut",
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
            "title": "ErrorOut",
            "type": "object"
          }
        }
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "a-riverhog-cli-result/collection/tag/remove/v1",
  "profile_id": "a-riverhog-cli-human-json/v1",
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
          "operation_id": "remove_collection_tag",
          "path": "/v1/collections/{collection_id}/tags:remove",
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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/remove/terminating_controls`

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
