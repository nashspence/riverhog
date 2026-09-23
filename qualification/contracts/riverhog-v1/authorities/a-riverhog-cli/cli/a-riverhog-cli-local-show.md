# a-riverhog-cli local show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-local-show:c742b252a0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-99dc954c7f"></a>Parser name: `show`
- <a id="s-9d8eb4f259"></a>Extra arguments at this parser: rejected.
- <a id="s-9e403e7b65"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-6db2d92636"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-50a72226fc"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-9768319a20"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-0fca28b679"></a>`help` | <a id="s-c94033adc4"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c26b8af917"></a>`0` | <a id="s-985f474fb9"></a>`"noncontractual-framework-help"` | <a id="s-5350ec910b"></a>`"empty"` |

### Result and failure contract

- <a id="s-70f9e5a336"></a>Result identity: `a-riverhog-cli-result/local/show/v1`
- <a id="s-ed059764e5"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-a73827312c"></a>Structured output: `optional-json`
- <a id="s-0e45277cad"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-bdce55447d"></a>`completed` | <a id="s-928d352c76"></a>`{"kind":"command-completed"}` | <a id="s-97b7fbef73"></a>`0` | <a id="s-e882f0b30b"></a>human: `"noncontractual-presentation-of-command-result"`; json: [a-riverhog-cli-local-collection/v1](#s-ba44dbdaf5) | <a id="s-8b3b05278b"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-e9c82ae026"></a>`usage` | <a id="s-d0bcb8e21a"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-0fde898db0"></a>`2` | <a id="s-d71d45a96e"></a>all: `"empty"` | <a id="s-e8846967d8"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-dacd2191ff"></a>`operational` | <a id="s-107837c446"></a>`{"kind":"application-error"}` | <a id="s-ed29075f44"></a>`1` | <a id="s-f77e019e14"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-ab62152702"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-ba44dbdaf5"></a>`a-riverhog-cli-local-collection/v1`

Applies to: completed · stdout (json).

<a id="s-5042415b5c"></a>

- <a id="s-45cd8785ae"></a>`type`: `"object"`
- <a id="s-7372c32948"></a>`additionalProperties`: `false`
- <a id="s-a46fe1ef46"></a>`required`: `["collection_id","created_at","tag_count","status","files","bytes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0d45d87715"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-0c36e4dfb8"></a>`collection_id` | yes | type="integer"; minimum=1 |  |
| <a id="s-2c99b2235b"></a>`created_at` | yes | type="string" |  |
| <a id="s-58d5278a2c"></a>`files` | yes | type="integer"; minimum=0 |  |
| <a id="s-80f1afce36"></a>`status` | yes | enum=["desired","remote-deleted","synchronizing"] |  |
| <a id="s-e205fe0ca0"></a>`tag_count` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-50a72226fc) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-9768319a20) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-16a049c3ef"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-1cbfe9b9a5"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/local/commands/show/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/show/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/show/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/show/name`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/show/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/show/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/show/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/show/result_contract`

<!-- exact-contract-value: 3e05d5069c359107e7cc63609a10afb5fe5bb88ab3592bebf92c50962f5f2f50 -->

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
  "identity": "a-riverhog-cli-result/local/show/v1",
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
          "identity": "a-riverhog-cli-local-collection/v1",
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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/show/terminating_controls`

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
