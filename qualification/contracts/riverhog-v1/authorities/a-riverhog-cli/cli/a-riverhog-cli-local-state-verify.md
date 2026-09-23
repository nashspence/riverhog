# a-riverhog-cli local state verify

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-local-state-verify:bd900aa89b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-3197388659"></a>Parser name: `verify`
- <a id="s-0fbae60a2c"></a>Extra arguments at this parser: rejected.
- <a id="s-3923bd9d6f"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-9e2afde60c"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-f298ad470a"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-d3228a8e12"></a>`help` | <a id="s-6433b243ff"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-e99f0cad21"></a>`0` | <a id="s-23240d3b0d"></a>`"noncontractual-framework-help"` | <a id="s-25bf4b943e"></a>`"empty"` |

### Result and failure contract

- <a id="s-0c51957bdc"></a>Result identity: `a-riverhog-cli-result/local/state/verify/v1`
- <a id="s-2d7552d49a"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-3398b70b91"></a>Structured output: `optional-json`
- <a id="s-92540f6c2b"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-afb3ad2c29"></a>`completed` | <a id="s-4ab3222fbf"></a>`{"kind":"command-completed"}` | <a id="s-ab53335845"></a>`0` | <a id="s-8a655742da"></a>human: `"noncontractual-presentation-of-command-result"`; json: [state-schema-status/v1](#s-5716998a19) | <a id="s-0bd948bfb9"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-025a6001e6"></a>`usage` | <a id="s-01d8fc48da"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-8199dba768"></a>`2` | <a id="s-2cc11555e9"></a>all: `"empty"` | <a id="s-f9bb47613b"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-41c743d12b"></a>`operational` | <a id="s-6f9cd2b10f"></a>`{"kind":"application-error"}` | <a id="s-dab54c0d64"></a>`1` | <a id="s-8765e6420f"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-cf7c250d9b"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-5716998a19"></a>`state-schema-status/v1`

Applies to: completed · stdout (json).

<a id="s-b7ceee002f"></a>

- <a id="s-1206cf010a"></a>`type`: `"object"`
- <a id="s-687f699f0b"></a>`additionalProperties`: `false`
- <a id="s-3ce367f171"></a>`required`: `["name","condition","current_revision","head_revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aa14bbb22a"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-85175fb8b5"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-2c4f59aa3e"></a>`head_revision` | yes | type="string" |  |
| <a id="s-0370fa7f3a"></a>`name` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-f298ad470a) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b41d453ad5"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-b45564d639"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/verify/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/verify/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/verify/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/verify/name`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/verify/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/verify/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/verify/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/verify/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/verify/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/verify/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/verify/name`

<!-- exact-contract-value: 898c74c2eed0452b1e51e567f237c37f1caa1e52f747466d56e76e15d07dc331 -->

```json
"verify"
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/verify/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/verify/result_contract`

<!-- exact-contract-value: 72272d0389ce9aae36027e7f2135efb84cae1363d1bba717e9db9ca7b67afc9f -->

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
  "identity": "a-riverhog-cli-result/local/state/verify/v1",
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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/commands/verify/terminating_controls`

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
