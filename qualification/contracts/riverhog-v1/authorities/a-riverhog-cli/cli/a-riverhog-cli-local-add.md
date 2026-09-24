# a-riverhog-cli local add

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-local-add:f2011e7c34 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-db08c67686"></a>Parser name: `add`
- <a id="s-911a8c45cf"></a>Extra arguments at this parser: rejected.
- <a id="s-2a99039765"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-251f5c1125"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-2a60d8df41"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-99aca7e6b1"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c981cc7412"></a>`help` | <a id="s-b2a9c858ad"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-8b3a2f50aa"></a>`0` | <a id="s-fbf7fe2df3"></a>`"noncontractual-framework-help"` | <a id="s-f5763c3f32"></a>`"empty"` |

### Result and failure contract

- <a id="s-fa87775476"></a>Result identity: `a-riverhog-cli-result/local/add/v1`
- <a id="s-53bf35fd3e"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-28fcd75d4b"></a>Structured output: `optional-json`
- <a id="s-3a41fa8e22"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-7d3b309561"></a>`completed` | <a id="s-504c298210"></a>`{"kind":"command-completed"}` | <a id="s-3f9abd3266"></a>`0` | <a id="s-c39e8bf6ce"></a>human: `"noncontractual-presentation-of-command-result"`; json: [a-riverhog-cli-local-add-result/v1](#s-6165a6ce57) | <a id="s-90124b8bbe"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b99133dcf4"></a>`usage` | <a id="s-390aded905"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-f8501d8f87"></a>`2` | <a id="s-97e2f3cfbc"></a>all: `"empty"` | <a id="s-39e4c687ef"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-708c937f37"></a>`operational` | <a id="s-ab9eb56955"></a>`{"kind":"application-error"}` | <a id="s-bd897f3db0"></a>`1` | <a id="s-c67499163e"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-f2c144417e"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-6165a6ce57"></a>`a-riverhog-cli-local-add-result/v1`

Applies to: completed · stdout (json).

<a id="s-5be9cdba50"></a>

- <a id="s-983a1c93a8"></a>`type`: `"object"`
- <a id="s-59ccf0e058"></a>`additionalProperties`: `false`
- <a id="s-cde77c373d"></a>`required`: `["status","collection"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collection` | yes | [See field `collection`](#s-6dd80ac305) |  |
| <a id="s-abb8137a51"></a>`status` | yes | const="added" |  |

##### <a id="s-6dd80ac305"></a>field `collection`

- <a id="s-9ff04d5aff"></a>`type`: `"object"`
- <a id="s-da86011899"></a>`additionalProperties`: `false`
- <a id="s-28f183d035"></a>`required`: `["collection_id","created_at","tag_count","status","files","bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0a9a453775"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-9ed3469c3c"></a>`collection_id` | yes | type="integer"; minimum=1 |  |
| <a id="s-7db324dd60"></a>`created_at` | yes | type="string" |  |
| <a id="s-9be5f4f3b6"></a>`files` | yes | type="integer"; minimum=0 |  |
| <a id="s-3705677ece"></a>`status` | yes | enum=["desired","remote-deleted","synchronizing"] |  |
| <a id="s-a34f71543c"></a>`tag_count` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-2a60d8df41) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-99aca7e6b1) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [GET /v1/catalog/collections/{collection_id}/inventory](../../riverhog/http-operations/get-v1-catalog-collections-collection-id-inventory.md)
- [GET /v1/collections/{collection_id}/tags](../../riverhog/http-operations/get-v1-collections-collection-id-tags.md)
- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [riverhog_client.ApiClient.get_collection](../../riverhog-client/python/riverhog-client-apiclient-get-collection.md)
- [riverhog_client.ApiClient.get_portable_collection_inventory](../../riverhog-client/python/riverhog-client-apiclient-get-portable-collection-inventory.md)
- [riverhog_client.ApiClient.list_collection_tags](../../riverhog-client/python/riverhog-client-apiclient-list-collection-tags.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c3eab4a59e"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-b87aa6d75b"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/local.py::add\_collection](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/local.py#L902)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/local/commands/add/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/add/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/add/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/add/name`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/add/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/add/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/add/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/add/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/add/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/add/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/add/name`

<!-- exact-contract-value: 7b8a6f33b43ca26a3f2aa73e408748f9ceb391ac21dfe746c94563016ab72f85 -->

```json
"add"
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/add/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/add/result_contract`

<!-- exact-contract-value: 8b5e5ca37287138a92689907858d9112fc8b7ac227e1224f4937d27c4bace766 -->

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
  "identity": "a-riverhog-cli-result/local/add/v1",
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
          "identity": "a-riverhog-cli-local-add-result/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "collection": {
                "additionalProperties": false,
                "properties": {
                  "bytes": {
                    "minimum": 0,
                    "type": "integer"
                  },
                  "collection_id": {
                    "minimum": 1,
                    "type": "integer"
                  },
                  "created_at": {
                    "type": "string"
                  },
                  "files": {
                    "minimum": 0,
                    "type": "integer"
                  },
                  "status": {
                    "enum": [
                      "desired",
                      "remote-deleted",
                      "synchronizing"
                    ]
                  },
                  "tag_count": {
                    "minimum": 0,
                    "type": "integer"
                  }
                },
                "required": [
                  "collection_id",
                  "created_at",
                  "tag_count",
                  "status",
                  "files",
                  "bytes"
                ],
                "type": "object"
              },
              "status": {
                "const": "added"
              }
            },
            "required": [
              "status",
              "collection"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/add/terminating_controls`

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
