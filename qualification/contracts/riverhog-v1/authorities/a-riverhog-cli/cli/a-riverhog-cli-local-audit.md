# a-riverhog-cli local audit

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-local-audit:04483cdb0a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-6cd4054aa8"></a>Parser name: `audit`
- <a id="s-03042d70f7"></a>Extra arguments at this parser: rejected.
- <a id="s-7586ef5f80"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-a70ef06039"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-17db8a6ef5"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c51788ca45"></a>`help` | <a id="s-188b3372af"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-5e5d7917b7"></a>`0` | <a id="s-08404855e5"></a>`"noncontractual-framework-help"` | <a id="s-4ff1b45313"></a>`"empty"` |

### Result and failure contract

- <a id="s-4e40f09b38"></a>Result identity: `a-riverhog-cli-result/local/audit/v1`
- <a id="s-c313274563"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-dc6c92ff53"></a>Structured output: `optional-json`
- <a id="s-65e1cd1c5f"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-173aec32b9"></a>`completed` | <a id="s-91d15a1948"></a>`{"kind":"command-completed"}` | <a id="s-811f9696bf"></a>`0` | <a id="s-fc6c780e53"></a>human: `"noncontractual-presentation-of-command-result"`; json: [a-riverhog-cli-local-audit-result/v1](#s-eb845586ae) | <a id="s-6dec9d015a"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-63b3f510ac"></a>`usage` | <a id="s-768ef59fa1"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-fb3ccb2a61"></a>`2` | <a id="s-0aa1b51028"></a>all: `"empty"` | <a id="s-717b0d0a0e"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-8dda433012"></a>`operational` | <a id="s-1555b840c7"></a>`{"kind":"application-error"}` | <a id="s-9c04a002d4"></a>`1` | <a id="s-79cc7164bf"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-a038d5a5b5"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |
| <a id="s-c7687ae947"></a>`audit-issues` | <a id="s-18d32d25df"></a>`{"kind":"local-audit-problem-count-positive"}` | <a id="s-ed7664acc3"></a>`1` | <a id="s-c041b77747"></a>human: `"noncontractual-presentation-of-command-result"`; json: [a-riverhog-cli-local-audit-result/v1](#s-12934f8fa8) | <a id="s-7c612016c2"></a>all: `"empty"` |

### Local structured outputs


#### <a id="s-eb845586ae"></a>`a-riverhog-cli-local-audit-result/v1`

Applies to: completed · stdout (json).

<a id="s-a483961c50"></a>

- <a id="s-84d1af380e"></a>`type`: `"object"`
- <a id="s-bc13496d47"></a>`additionalProperties`: `false`
- <a id="s-d34b12d39e"></a>`required`: `["status","problems","samples","samples_truncated"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4efc960ea2"></a>`problems` | yes | type="integer"; minimum=0 |  |
| <a id="s-170d42657e"></a>`samples` | yes | type="array"; items=(type="string"); maxItems=100 |  |
| <a id="s-870d1cc991"></a>`samples_truncated` | yes | type="boolean" |  |
| <a id="s-feff7e7667"></a>`status` | yes | enum=["ok","issues"] |  |

#### <a id="s-12934f8fa8"></a>`a-riverhog-cli-local-audit-result/v1`

Applies to: audit-issues · stdout (json).

<a id="s-a48be6e52f"></a>

- <a id="s-9f2edb43b4"></a>`type`: `"object"`
- <a id="s-0afad498f5"></a>`additionalProperties`: `false`
- <a id="s-1f0409eb15"></a>`required`: `["status","problems","samples","samples_truncated"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-620b0cd153"></a>`problems` | yes | type="integer"; minimum=0 |  |
| <a id="s-67d3256947"></a>`samples` | yes | type="array"; items=(type="string"); maxItems=100 |  |
| <a id="s-6e4496dbda"></a>`samples_truncated` | yes | type="boolean" |  |
| <a id="s-ebc799dc44"></a>`status` | yes | enum=["ok","issues"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-17db8a6ef5) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-48e5bc5fd7"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-af6791892e"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/local/commands/audit/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/audit/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/audit/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/audit/name`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/audit/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/audit/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/audit/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/audit/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/audit/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/audit/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/audit/name`

<!-- exact-contract-value: 8855233a0eba693696a41cfeda5ee29b535db82a64c521658514f94aa509be3f -->

```json
"audit"
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/audit/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/audit/result_contract`

<!-- exact-contract-value: 155a29ab61e8d18693f4f9895db87137ac399b169675b523f1d06c41cd5fa058 -->

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
    },
    {
      "exit_status": 1,
      "id": "audit-issues",
      "selected_by": {
        "kind": "local-audit-problem-count-positive"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "identity": "a-riverhog-cli-local-audit-result/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "problems": {
                "minimum": 0,
                "type": "integer"
              },
              "samples": {
                "items": {
                  "type": "string"
                },
                "maxItems": 100,
                "type": "array"
              },
              "samples_truncated": {
                "type": "boolean"
              },
              "status": {
                "enum": [
                  "ok",
                  "issues"
                ]
              }
            },
            "required": [
              "status",
              "problems",
              "samples",
              "samples_truncated"
            ],
            "type": "object"
          }
        }
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "a-riverhog-cli-result/local/audit/v1",
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
          "identity": "a-riverhog-cli-local-audit-result/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "problems": {
                "minimum": 0,
                "type": "integer"
              },
              "samples": {
                "items": {
                  "type": "string"
                },
                "maxItems": 100,
                "type": "array"
              },
              "samples_truncated": {
                "type": "boolean"
              },
              "status": {
                "enum": [
                  "ok",
                  "issues"
                ]
              }
            },
            "required": [
              "status",
              "problems",
              "samples",
              "samples_truncated"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/audit/terminating_controls`

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
