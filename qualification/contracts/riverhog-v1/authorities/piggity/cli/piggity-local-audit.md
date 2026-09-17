# piggity local audit

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-audit:6bf8603d44 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-de923e4a33"></a>Parser name: `audit`
- <a id="s-a2f3f065fc"></a>Extra arguments at this parser: rejected.
- <a id="s-d0a73d56b3"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-9d18892d9b"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-2df9614c58"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-a5ef839028"></a>`help` | <a id="s-3114b5fdd1"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-2906dcb7b6"></a>`0` | <a id="s-2cd21f6187"></a>`"noncontractual-framework-help"` | <a id="s-030ee1a51c"></a>`"empty"` |

### Result and failure contract

- <a id="s-894654c086"></a>Result identity: `piggity-cli-result/local/audit/v1`
- <a id="s-373c48e071"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-ddc45d65e2"></a>Structured output: `optional-json`
- <a id="s-95894ad3ab"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b2c9087580"></a>`completed` | <a id="s-7270ded0ea"></a>`{"kind":"command-completed"}` | <a id="s-8cc110a0a6"></a>`0` | <a id="s-7923b371d9"></a>human: `"noncontractual-presentation-of-command-result"`; json: [piggity-local-audit-result/v1](#s-d674179474) | <a id="s-61aa49cd92"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f9a7b01ee7"></a>`usage` | <a id="s-0971b08506"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-e09219d1f2"></a>`2` | <a id="s-d5f38b7b34"></a>all: `"empty"` | <a id="s-64a8941ac9"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-f6e8c9c6bc"></a>`operational` | <a id="s-6d7826f892"></a>`{"kind":"application-error"}` | <a id="s-cb91e9abd4"></a>`1` | <a id="s-d682d26a84"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-6830214707"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |
| <a id="s-3085f2c952"></a>`audit-issues` | <a id="s-8ada29b221"></a>`{"kind":"local-audit-problem-count-positive"}` | <a id="s-fad0414f0a"></a>`1` | <a id="s-cd7eb3d6da"></a>human: `"noncontractual-presentation-of-command-result"`; json: [piggity-local-audit-result/v1](#s-03028cc2c8) | <a id="s-0d1aefa173"></a>all: `"empty"` |

### Local structured outputs


#### <a id="s-d674179474"></a>`piggity-local-audit-result/v1`

Applies to: completed · stdout (json).

<a id="s-6879261c97"></a>

- <a id="s-6ff87e9546"></a>`type`: `"object"`
- <a id="s-e21e42dc4a"></a>`additionalProperties`: `false`
- <a id="s-67429944a4"></a>`required`: `["status","problems","samples","samples_truncated"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-46ddb3296a"></a>`problems` | yes | type="integer"; minimum=0 |  |
| <a id="s-7a3698d54a"></a>`samples` | yes | type="array"; items=(type="string"); maxItems=100 |  |
| <a id="s-c0130ab445"></a>`samples_truncated` | yes | type="boolean" |  |
| <a id="s-7932ae4ee0"></a>`status` | yes | enum=["ok","issues"] |  |

#### <a id="s-03028cc2c8"></a>`piggity-local-audit-result/v1`

Applies to: audit-issues · stdout (json).

<a id="s-248a3c4b1f"></a>

- <a id="s-78625681fd"></a>`type`: `"object"`
- <a id="s-be5d45912d"></a>`additionalProperties`: `false`
- <a id="s-43ba127ead"></a>`required`: `["status","problems","samples","samples_truncated"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-69b70b1835"></a>`problems` | yes | type="integer"; minimum=0 |  |
| <a id="s-7d444e010f"></a>`samples` | yes | type="array"; items=(type="string"); maxItems=100 |  |
| <a id="s-b60401df2c"></a>`samples_truncated` | yes | type="boolean" |  |
| <a id="s-ce9bc0089e"></a>`status` | yes | enum=["ok","issues"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-2df9614c58) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-68b1c4ca6b"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-f76a6ff07c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/audit/allow_extra_args`
- `/external_contract/cli/piggity/commands/local/commands/audit/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/local/commands/audit/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/local/commands/audit/name`
- `/external_contract/cli/piggity/commands/local/commands/audit/parameters`
- `/external_contract/cli/piggity/commands/local/commands/audit/result_contract`
- `/external_contract/cli/piggity/commands/local/commands/audit/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/audit/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/audit/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/local/commands/audit/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/audit/name`

<!-- exact-contract-value: 8855233a0eba693696a41cfeda5ee29b535db82a64c521658514f94aa509be3f -->

```json
"audit"
```

### `/external_contract/cli/piggity/commands/local/commands/audit/parameters`

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

### `/external_contract/cli/piggity/commands/local/commands/audit/result_contract`

<!-- exact-contract-value: 5f837e026a5ae9c87b0a9b45105199c347c5aedac8ba76fe5d33fa7adaf7b943 -->

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
          "identity": "piggity-local-audit-result/v1",
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
  "identity": "piggity-cli-result/local/audit/v1",
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
          "identity": "piggity-local-audit-result/v1",
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

### `/external_contract/cli/piggity/commands/local/commands/audit/terminating_controls`

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
