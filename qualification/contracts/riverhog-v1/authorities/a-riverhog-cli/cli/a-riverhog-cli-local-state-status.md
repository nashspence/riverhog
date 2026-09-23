# a-riverhog-cli local state status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-local-state-status:9bd00174fc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-6302e9769e"></a>Parser name: `status`
- <a id="s-d493448a1a"></a>Extra arguments at this parser: rejected.
- <a id="s-a880d4efcf"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-8755dbe4d6"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-f03e8bbee7"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-e7345ee8b6"></a>`help` | <a id="s-94e51b70fc"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-d953c219a4"></a>`0` | <a id="s-db7832e8d6"></a>`"noncontractual-framework-help"` | <a id="s-6571d6a8d3"></a>`"empty"` |

### Result and failure contract

- <a id="s-51781c0169"></a>Result identity: `a-riverhog-cli-result/local/state/status/v1`
- <a id="s-044f1be0ab"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-6684c47fb1"></a>Structured output: `optional-json`
- <a id="s-6b1c855cf7"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5ab3c92cbf"></a>`completed` | <a id="s-a6d5b4ee33"></a>`{"kind":"command-completed"}` | <a id="s-4031063128"></a>`0` | <a id="s-3959147e8e"></a>human: `"noncontractual-presentation-of-command-result"`; json: [state-schema-status/v1](#s-52d6a50381) | <a id="s-bc2477b452"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-75f17fd203"></a>`usage` | <a id="s-9c1042c5b3"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-322420d686"></a>`2` | <a id="s-f9417f47a0"></a>all: `"empty"` | <a id="s-1169b085ba"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-fcc1183685"></a>`operational` | <a id="s-5aadf8f4c5"></a>`{"kind":"application-error"}` | <a id="s-e9c48e8c8e"></a>`1` | <a id="s-0ea69275c2"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-8009cbc7d5"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-52d6a50381"></a>`state-schema-status/v1`

Applies to: completed · stdout (json).

<a id="s-3a8a878be7"></a>

- <a id="s-0eeb2d36a8"></a>`type`: `"object"`
- <a id="s-5abefdb198"></a>`additionalProperties`: `false`
- <a id="s-a1e663fd9e"></a>`required`: `["name","condition","current_revision","head_revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-22d569ea36"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-dd63f2219d"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-d7be0c5928"></a>`head_revision` | yes | type="string" |  |
| <a id="s-7e7a5988e8"></a>`name` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-f03e8bbee7) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-4a32830247"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-5b41e5073a"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/status/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/status/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/status/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/status/name`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/status/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/status/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/status/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/status/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/status/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/status/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/status/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/status/result_contract`

<!-- exact-contract-value: f527bb4d1aa2bf44d8da0cc8220d10b0854c675c797b20749e0f1c5cf8a98c57 -->

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
  "identity": "a-riverhog-cli-result/local/state/status/v1",
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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/status/terminating_controls`

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
