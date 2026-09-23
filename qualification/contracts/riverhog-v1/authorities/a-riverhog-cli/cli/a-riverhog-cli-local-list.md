# a-riverhog-cli local list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-local-list:c915a23625 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-be40100151"></a>Parser name: `list`
- <a id="s-48fa319778"></a>Extra arguments at this parser: rejected.
- <a id="s-c9fd9ad63b"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-ab4e984a36"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-67a36d3663"></a>`page_size`<br>`--page-size` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `25`<br>Env: `null` |
| <a id="s-a9f8cd8627"></a>`page_token`<br>`--page-token` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-be14209ee6"></a>`sort`<br>`--sort` | optional option; 1 value | text | `"collection_id"`<br>Env: `null` |
| <a id="s-d2833b5e3f"></a>`order`<br>`--order` | optional option; 1 value | text | `"asc"`<br>Env: `null` |
| <a id="s-d3ad6a3103"></a>`query`<br>`--query`, `-q` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-b295e744af"></a>`ids`<br>`--ids` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-8cc0de3c1c"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-40fb329938"></a>`help` | <a id="s-63c903b307"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-6cddb325ef"></a>`0` | <a id="s-2d1fd1dc5a"></a>`"noncontractual-framework-help"` | <a id="s-decad9234e"></a>`"empty"` |

### Result and failure contract

- <a id="s-47163fb1e8"></a>Result identity: `a-riverhog-cli-result/local/list/v1`
- <a id="s-a1d76548c5"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-7c3fe7fcbd"></a>Structured output: `optional-json`
- <a id="s-067e59b8fa"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-87cda05f35"></a>`completed` | <a id="s-68dfe627df"></a>`{"kind":"command-completed"}` | <a id="s-fc9e9779b0"></a>`0` | <a id="s-0e1c1e5e88"></a>human: `"noncontractual-presentation-of-command-result"`; json: [a-riverhog-cli-local-collection-list/v1](#s-1ab2ac76e0) | <a id="s-0ed3be0ded"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-920d3ec72e"></a>`usage` | <a id="s-7cc5601ebb"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-a87adfb4e9"></a>`2` | <a id="s-6de5adbbb6"></a>all: `"empty"` | <a id="s-c3b86313b9"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-8b62f75bed"></a>`operational` | <a id="s-b6a878a937"></a>`{"kind":"application-error"}` | <a id="s-b5d617d3f6"></a>`1` | <a id="s-bdb2a710a9"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-0c310f9955"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-1ab2ac76e0"></a>`a-riverhog-cli-local-collection-list/v1`

Applies to: completed · stdout (json).

<a id="s-ea8041bd97"></a>

- <a id="s-3d4df7bd8a"></a>`type`: `"object"`
- <a id="s-9c51d2fa88"></a>`additionalProperties`: `false`
- <a id="s-7c0c921d47"></a>`required`: `["page_size","next_page_token","sort","order","query","collections"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collections` | yes | [See field `collections`](#s-94d43f1f0d) |  |
| <a id="s-0f71860a61"></a>`next_page_token` | yes | type=["string","null"] |  |
| <a id="s-b850e2d3aa"></a>`order` | yes | enum=["asc","desc"] |  |
| <a id="s-6e7254fc01"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-c7631134da"></a>`query` | yes | type=["string","null"] |  |
| <a id="s-31746caead"></a>`sort` | yes | enum=["bytes","collection_id","created_at","files","status"] |  |

##### <a id="s-94d43f1f0d"></a>field `collections`

- <a id="s-b3ec6fde67"></a>`type`: `"array"`
- `items`: [See field `collections` · `items`](#s-2a2bbe66dc)

##### <a id="s-2a2bbe66dc"></a>field `collections` · `items`

- <a id="s-03e4b2bbb4"></a>`type`: `"object"`
- <a id="s-34eb653c8a"></a>`additionalProperties`: `false`
- <a id="s-f5ca6de046"></a>`required`: `["collection_id","created_at","tag_count","status","files","bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6b2a7e1682"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-00f1d63e3d"></a>`collection_id` | yes | type="integer"; minimum=1 |  |
| <a id="s-138d312a9b"></a>`created_at` | yes | type="string" |  |
| <a id="s-16a8c5f28d"></a>`files` | yes | type="integer"; minimum=0 |  |
| <a id="s-a6a66aefcf"></a>`status` | yes | enum=["desired","remote-deleted","synchronizing"] |  |
| <a id="s-10c71312d5"></a>`tag_count` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --ids](#s-b295e744af) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --json](#s-8cc0de3c1c) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --order](#s-d2833b5e3f) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-size](#s-67a36d3663) | `value · cli-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-67a36d3663) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-a9f8cd8627) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --query](#s-d3ad6a3103) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --sort](#s-be14209ee6) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-e4b51f129f"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-5ff8a81848"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/local/commands/list/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/list/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/list/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/list/name`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/list/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/list/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/list/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/list/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/list/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/list/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/list/parameters`

<!-- exact-contract-value: 8344c847f3714130088f7f341e7e6510a2c2bf6135cbe2ce008d448e6bc7f32b -->

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
      "clamp": false,
      "class": "typer._click.types.IntRange",
      "max_open": false,
      "maximum": 100,
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
    "default": "collection_id",
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
    "default": "asc",
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
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "ids",
    "nargs": 1,
    "options": [
      "--ids"
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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/list/result_contract`

<!-- exact-contract-value: 300e3b426bf6829329c4be870190f9aafddd4195d9a4c76b35fd0b1ab2bade91 -->

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
  "identity": "a-riverhog-cli-result/local/list/v1",
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
          "identity": "a-riverhog-cli-local-collection-list/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "collections": {
                "items": {
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
                "type": "array"
              },
              "next_page_token": {
                "type": [
                  "string",
                  "null"
                ]
              },
              "order": {
                "enum": [
                  "asc",
                  "desc"
                ]
              },
              "page_size": {
                "maximum": 100,
                "minimum": 1,
                "type": "integer"
              },
              "query": {
                "type": [
                  "string",
                  "null"
                ]
              },
              "sort": {
                "enum": [
                  "bytes",
                  "collection_id",
                  "created_at",
                  "files",
                  "status"
                ]
              }
            },
            "required": [
              "page_size",
              "next_page_token",
              "sort",
              "order",
              "query",
              "collections"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/list/terminating_controls`

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
