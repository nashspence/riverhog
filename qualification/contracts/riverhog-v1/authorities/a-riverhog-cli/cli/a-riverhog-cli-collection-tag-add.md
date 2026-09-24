# a-riverhog-cli collection tag add

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-tag-add:2a2d61f8ee -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-0e3ca11a85"></a>Parser name: `add`
- <a id="s-28eaf8f1de"></a>Extra arguments at this parser: rejected.
- <a id="s-479b5c581b"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-b59856899d"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-b2a5fc003f"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-17f390a53b"></a>`tag`<br>`tag` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-43329a1073"></a>`revision`<br>`--revision` | optional option; 1 value | integer range; minimum=`1` (inclusive); outside range: reject | not recorded<br>Env: `null` |
| <a id="s-52713757d5"></a>`tag_set_identity`<br>`--tag-set-identity` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-39456ab1a8"></a>`operation_id`<br>`--operation-id` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-dd3ff4d718"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-f052334f2e"></a>`help` | <a id="s-543803e198"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-a6d7ce73b5"></a>`0` | <a id="s-355edac5e5"></a>`"noncontractual-framework-help"` | <a id="s-40d815923e"></a>`"empty"` |

### Result and failure contract

- <a id="s-e7fb3050f2"></a>Result identity: `a-riverhog-cli-result/collection/tag/add/v1`
- <a id="s-c93584c263"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-c832b3ba43"></a>Structured output: `optional-json`
- <a id="s-206a74439a"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-019dc805a3"></a>`completed` | <a id="s-2d007d6616"></a>`{"kind":"command-completed"}` | <a id="s-9ea8ec4689"></a>`0` | <a id="s-5c0b150e22"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP add_collection_tag response 200](../../riverhog/http-operations/post-v1-collections-collection-id-tags-add.md#s-293ae74875) | <a id="s-59a2c563fa"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-7ed865f32d"></a>`usage` | <a id="s-c9b329b556"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-d6491add65"></a>`2` | <a id="s-28741966de"></a>all: `"empty"` | <a id="s-b49d384f47"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-16b73d996e"></a>`operational` | <a id="s-e48aee2777"></a>`{"kind":"application-error"}` | <a id="s-3a40a27ff7"></a>`1` | <a id="s-b4570b71b8"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-c43d050141"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-b2a5fc003f) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-dd3ff4d718) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --operation-id](#s-39456ab1a8) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --revision](#s-43329a1073) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter tag](#s-17f390a53b) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --tag-set-identity](#s-52713757d5) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [POST /v1/collections/{collection_id}/tags:add](../../riverhog/http-operations/post-v1-collections-collection-id-tags-add.md)
- [riverhog_client.ApiClient.add_collection_tag](../../riverhog-client/python/riverhog-client-apiclient-add-collection-tag.md)
- [riverhog_client.ApiClient.get_collection](../../riverhog-client/python/riverhog-client-apiclient-get-collection.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-55a17e6b22"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-736613baa0"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::collection\_tag\_add\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L1059)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/add/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/add/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/add/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/add/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/add/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/add/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/add/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/add/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/add/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/add/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/add/name`

<!-- exact-contract-value: 7b8a6f33b43ca26a3f2aa73e408748f9ceb391ac21dfe746c94563016ab72f85 -->

```json
"add"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/add/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/add/result_contract`

<!-- exact-contract-value: 9844023f2809583e1332ef8b0a1308045dbf504cd723d15bc89feefa0a326802 -->

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
  "identity": "a-riverhog-cli-result/collection/tag/add/v1",
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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/add/terminating_controls`

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
