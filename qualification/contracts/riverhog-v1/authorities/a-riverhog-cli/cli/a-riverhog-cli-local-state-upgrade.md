# a-riverhog-cli local state upgrade

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-local-state-upgrade:f7e733467a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-102463b087"></a>Parser name: `upgrade`
- <a id="s-e5c20b6767"></a>Extra arguments at this parser: rejected.
- <a id="s-88170db358"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-4b6de6ca9f"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-712e339df6"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-efffbf096e"></a>`help` | <a id="s-33c6a91d2a"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-5eeaf9205b"></a>`0` | <a id="s-2ac463578a"></a>`"noncontractual-framework-help"` | <a id="s-fd6f10f40e"></a>`"empty"` |

### Result and failure contract

- <a id="s-f53e01ac6e"></a>Result identity: `a-riverhog-cli-result/local/state/upgrade/v1`
- <a id="s-7252e67a16"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-5de5949b54"></a>Structured output: `optional-json`
- <a id="s-65efaa277e"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b25a8c4726"></a>`completed` | <a id="s-594ac7d16e"></a>`{"kind":"command-completed"}` | <a id="s-440d2c6533"></a>`0` | <a id="s-ab20741018"></a>human: `"noncontractual-presentation-of-command-result"`; json: [state-schema-status/v1](#s-90397fed21) | <a id="s-7ba8dc404e"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-baeaa03077"></a>`usage` | <a id="s-9670b2ef3e"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-5b4ce33f49"></a>`2` | <a id="s-84c10bf60b"></a>all: `"empty"` | <a id="s-effa487f2e"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-6396f35d17"></a>`operational` | <a id="s-f39301e4a5"></a>`{"kind":"application-error"}` | <a id="s-a702b0fd3f"></a>`1` | <a id="s-3570b0da7d"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-eb57ad7217"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-90397fed21"></a>`state-schema-status/v1`

Applies to: completed · stdout (json).

<a id="s-3b63bb3658"></a>

- <a id="s-c69abfb50b"></a>`type`: `"object"`
- <a id="s-c2c36376b9"></a>`additionalProperties`: `false`
- <a id="s-861b706328"></a>`required`: `["name","condition","current_revision","head_revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1791a08bca"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-7dff51da6e"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-6f87b5aeea"></a>`head_revision` | yes | type="string" |  |
| <a id="s-0faafdb32c"></a>`name` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-712e339df6) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-6d67b1c6ee"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-fe50c672a8"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/upgrade/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/upgrade/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/upgrade/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/upgrade/name`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/upgrade/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/upgrade/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/upgrade/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/upgrade/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/upgrade/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/upgrade/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/upgrade/name`

<!-- exact-contract-value: 192e80d1fe4e27d140b2db67853ff131d7a670e024c440098f759d2df9f2c230 -->

```json
"upgrade"
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/upgrade/parameters`

<!-- exact-contract-value: f2cf9ed04ac608b58219dbcf22fc63be2fdf35901bc058f443df21b229aefd32 -->

```json
[
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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/upgrade/result_contract`

<!-- exact-contract-value: 37c0eb29084667d8963f513f15196d21f7b64483dc890d7f3acadde6841a0217 -->

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
  "identity": "a-riverhog-cli-result/local/state/upgrade/v1",
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
          "identity": "state-schema-status/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "condition": {
                "enum": [
                  "empty",
                  "current",
                  "upgrade_required",
                  "unversioned",
                  "incompatible"
                ]
              },
              "current_revision": {
                "type": [
                  "string",
                  "null"
                ]
              },
              "head_revision": {
                "type": "string"
              },
              "name": {
                "type": "string"
              }
            },
            "required": [
              "name",
              "condition",
              "current_revision",
              "head_revision"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/upgrade/terminating_controls`

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
