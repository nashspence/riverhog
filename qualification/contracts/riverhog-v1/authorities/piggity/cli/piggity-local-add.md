# piggity local add

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-add:1fa4ec273a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-87bd24aa1c"></a>Parser name: `add`
- <a id="s-76e1e1a607"></a>Extra arguments at this parser: rejected.
- <a id="s-ad1b58892c"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-010a89bbd3"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-6667bacd77"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-540954d732"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-2a29fc205c"></a>`help` | <a id="s-11733984c1"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c96bfa7ee6"></a>`0` | <a id="s-14fa5167c3"></a>`"noncontractual-framework-help"` | <a id="s-0115fa990e"></a>`"empty"` |

### Result and failure contract

- <a id="s-ec423bc633"></a>Result identity: `piggity-cli-result/local/add/v1`
- <a id="s-d23ae6df0b"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-29790aa569"></a>Structured output: `optional-json`
- <a id="s-5981a56bb9"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-ba8f17cf23"></a>`completed` | <a id="s-6191846f61"></a>`{"kind":"command-completed"}` | <a id="s-2ca0d4c6a2"></a>`0` | <a id="s-3ec7696914"></a>human: `"noncontractual-presentation-of-command-result"`; json: [piggity-local-add-result/v1](#s-47c1b8cdd2) | <a id="s-cf05916a92"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-85bcf7d329"></a>`usage` | <a id="s-22cfb407fa"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-1f5517d615"></a>`2` | <a id="s-70e77ea78a"></a>all: `"empty"` | <a id="s-d0209239bb"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-65f80b576a"></a>`operational` | <a id="s-67407cc036"></a>`{"kind":"application-error"}` | <a id="s-0b5581bf48"></a>`1` | <a id="s-9bed62b9aa"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-d436be9c29"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-47c1b8cdd2"></a>`piggity-local-add-result/v1`

Applies to: completed · stdout (json).

<a id="s-c5d3cfb131"></a>

- <a id="s-1a4b63fbd4"></a>`type`: `"object"`
- <a id="s-74be903758"></a>`additionalProperties`: `false`
- <a id="s-69d375ed7d"></a>`required`: `["status","collection"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collection` | yes | [See field `collection`](#s-e8c315b9e3) |  |
| <a id="s-04276f05ce"></a>`status` | yes | const="added" |  |

##### <a id="s-e8c315b9e3"></a>field `collection`

- <a id="s-375e0c6192"></a>`type`: `"object"`
- <a id="s-c8421fae82"></a>`additionalProperties`: `false`
- <a id="s-882efc6c00"></a>`required`: `["collection_id","created_at","tag_count","status","files","bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a82c809166"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-71ceee83b8"></a>`collection_id` | yes | type="integer"; minimum=1 |  |
| <a id="s-97746b45e1"></a>`created_at` | yes | type="string" |  |
| <a id="s-5351954972"></a>`files` | yes | type="integer"; minimum=0 |  |
| <a id="s-bb68e09549"></a>`status` | yes | enum=["desired","remote-deleted","synchronizing"] |  |
| <a id="s-1d8a8a317e"></a>`tag_count` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-6667bacd77) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-540954d732) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [GET /v1/catalog/collections/{collection_id}/inventory](../../riverhog/http-operations/get-v1-catalog-collections-collection-id-inventory.md)
- [GET /v1/collections/{collection_id}/tags](../../riverhog/http-operations/get-v1-collections-collection-id-tags.md)
- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [riverhog_client.ApiClient.get_collection](../../riverhog-client/python/riverhog-client-apiclient-get-collection.md)
- [riverhog_client.ApiClient.get_portable_collection_inventory](../../riverhog-client/python/riverhog-client-apiclient-get-portable-collection-inventory.md)
- [riverhog_client.ApiClient.list_collection_tags](../../riverhog-client/python/riverhog-client-apiclient-list-collection-tags.md)

## Governing policies

- <a id="pa-0ae575b104"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-eb988d9903"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources/authorities.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/local.py::add\_collection](../../../../../../reference/riverhog/applications/piggity/src/piggity/local.py#L902)

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/add/allow_extra_args`
- `/external_contract/cli/piggity/commands/local/commands/add/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/local/commands/add/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/local/commands/add/name`
- `/external_contract/cli/piggity/commands/local/commands/add/parameters`
- `/external_contract/cli/piggity/commands/local/commands/add/result_contract`
- `/external_contract/cli/piggity/commands/local/commands/add/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/add/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/add/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/local/commands/add/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/add/name`

<!-- exact-contract-value: 7b8a6f33b43ca26a3f2aa73e408748f9ceb391ac21dfe746c94563016ab72f85 -->

```json
"add"
```

### `/external_contract/cli/piggity/commands/local/commands/add/parameters`

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

### `/external_contract/cli/piggity/commands/local/commands/add/result_contract`

<!-- exact-contract-value: 8a1f84c5ebaad5b02c7afacd5391137f69a16ee0dc342d6157b1fe395ca67b5f -->

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
  "identity": "piggity-cli-result/local/add/v1",
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
          "identity": "piggity-local-add-result/v1",
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

### `/external_contract/cli/piggity/commands/local/commands/add/terminating_controls`

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
