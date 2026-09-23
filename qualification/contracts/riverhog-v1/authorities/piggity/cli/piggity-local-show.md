# piggity local show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-show:93e5ac56cc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-b4ec570213"></a>Parser name: `show`
- <a id="s-5cfe11f90d"></a>Extra arguments at this parser: rejected.
- <a id="s-bd40cf1c88"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-3d5ef0c2cf"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-a248dfb131"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-23ed8f5775"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-ecc2c1467d"></a>`help` | <a id="s-82ebc2e19b"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c59ffeddde"></a>`0` | <a id="s-d93a3e0cd2"></a>`"noncontractual-framework-help"` | <a id="s-314f2d65f4"></a>`"empty"` |

### Result and failure contract

- <a id="s-3708e68335"></a>Result identity: `piggity-cli-result/local/show/v1`
- <a id="s-1b7b833700"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-a11f266a7e"></a>Structured output: `optional-json`
- <a id="s-abff583df4"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-765f56719e"></a>`completed` | <a id="s-f6cf31a51d"></a>`{"kind":"command-completed"}` | <a id="s-a0e83a57d6"></a>`0` | <a id="s-3507067d4b"></a>human: `"noncontractual-presentation-of-command-result"`; json: [piggity-local-collection/v1](#s-a60947f659) | <a id="s-0e725e0b06"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c3a8194b69"></a>`usage` | <a id="s-88e4288517"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-637f8ad8dd"></a>`2` | <a id="s-3e2b337274"></a>all: `"empty"` | <a id="s-b90cc9abc4"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-704057c776"></a>`operational` | <a id="s-21214c04ca"></a>`{"kind":"application-error"}` | <a id="s-cb50b2d48a"></a>`1` | <a id="s-188d15e271"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-941dcf6548"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-a60947f659"></a>`piggity-local-collection/v1`

Applies to: completed · stdout (json).

<a id="s-34cff38a4c"></a>

- <a id="s-7169af7875"></a>`type`: `"object"`
- <a id="s-18b0a727b0"></a>`additionalProperties`: `false`
- <a id="s-739302180c"></a>`required`: `["collection_id","created_at","tag_count","status","files","bytes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-90ed26ff5f"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-0c165b7977"></a>`collection_id` | yes | type="integer"; minimum=1 |  |
| <a id="s-c55695f952"></a>`created_at` | yes | type="string" |  |
| <a id="s-78cbdeaa35"></a>`files` | yes | type="integer"; minimum=0 |  |
| <a id="s-1b19aaaf80"></a>`status` | yes | enum=["desired","remote-deleted","synchronizing"] |  |
| <a id="s-2340f25e26"></a>`tag_count` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-a248dfb131) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-23ed8f5775) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-5ab3380de6"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-5ac2fc50d9"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources/authorities.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/show/allow_extra_args`
- `/external_contract/cli/piggity/commands/local/commands/show/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/local/commands/show/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/local/commands/show/name`
- `/external_contract/cli/piggity/commands/local/commands/show/parameters`
- `/external_contract/cli/piggity/commands/local/commands/show/result_contract`
- `/external_contract/cli/piggity/commands/local/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/local/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/piggity/commands/local/commands/show/parameters`

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

### `/external_contract/cli/piggity/commands/local/commands/show/result_contract`

<!-- exact-contract-value: 07dc50fdb035c09fb5dde7123a83b04cbff25678faa53ebbd740a6b46b372855 -->

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
  "identity": "piggity-cli-result/local/show/v1",
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
          "identity": "piggity-local-collection/v1",
          "kind": "cli-local-json-schema",
          "schema": {
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
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/local/commands/show/terminating_controls`

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
