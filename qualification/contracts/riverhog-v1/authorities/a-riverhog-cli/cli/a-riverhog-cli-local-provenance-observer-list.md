# a-riverhog-cli local provenance-observer list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-local-provenance-observer-list:4010adf449 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-c2639c3e82"></a>Parser name: `list`
- <a id="s-51c6751f2f"></a>Extra arguments at this parser: rejected.
- <a id="s-f3c67321ed"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-af734ffd70"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-6b0bb6db55"></a>`ids`<br>`--ids` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-05e88b6ac0"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c618f19c0d"></a>`help` | <a id="s-7f27c21445"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-791a88527b"></a>`0` | <a id="s-043a51e1df"></a>`"noncontractual-framework-help"` | <a id="s-f8ee74bcbe"></a>`"empty"` |

### Result and failure contract

- <a id="s-3bd1fba5ce"></a>Result identity: `a-riverhog-cli-result/local/provenance-observer/list/v1`
- <a id="s-bda5c03a3b"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-58a0e00ca0"></a>Structured output: `optional-json`
- <a id="s-da6f97966b"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a9d10d4dcc"></a>`completed` | <a id="s-c987892750"></a>`{"kind":"command-completed"}` | <a id="s-6173553bc0"></a>`0` | <a id="s-56117f0bf7"></a>human: `"noncontractual-presentation-of-command-result"`; json: [riverhog-provenance-observer-provider-list/v1](#s-b1cf531d09) | <a id="s-b2c05661a3"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-aba5c8af1f"></a>`usage` | <a id="s-d33e2b8528"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-7c65579e09"></a>`2` | <a id="s-86c325515a"></a>all: `"empty"` | <a id="s-634d0c5a33"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-6a53994f98"></a>`operational` | <a id="s-9f9f49508d"></a>`{"kind":"application-error"}` | <a id="s-e85809d833"></a>`1` | <a id="s-e4e30a43b8"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-96779e7382"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-b1cf531d09"></a>`riverhog-provenance-observer-provider-list/v1`

Applies to: completed · stdout (json).

<a id="s-0e0b5876dd"></a>

- <a id="s-7e426f9698"></a>`type`: `"object"`
- <a id="s-c6e17e836a"></a>`additionalProperties`: `false`
- <a id="s-9e67203d86"></a>`required`: `["format","providers"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a0a3824473"></a>`format` | yes | const="riverhog-provenance-observer-provider-list/v1" |  |
| <a id="s-11609bc772"></a>`providers` | yes | type="array"; items=(type="object") |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --ids](#s-6b0bb6db55) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-05e88b6ac0) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-e9b70bb284"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-39d4184074"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/list/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/list/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/list/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/list/name`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/list/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/list/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/list/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/list/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/list/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/list/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/list/parameters`

<!-- exact-contract-value: e1d69a72acbf63fdc3459d233dcdf028761244a8bf1785d2a24bd76449d6da79 -->

```json
[
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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/list/result_contract`

<!-- exact-contract-value: 785de57dbba8a62862d7d90778c19c918ab6670b63e1802a17e11ce9885b2c2e -->

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
  "identity": "a-riverhog-cli-result/local/provenance-observer/list/v1",
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
          "identity": "riverhog-provenance-observer-provider-list/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "format": {
                "const": "riverhog-provenance-observer-provider-list/v1"
              },
              "providers": {
                "items": {
                  "type": "object"
                },
                "type": "array"
              }
            },
            "required": [
              "format",
              "providers"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/list/terminating_controls`

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
