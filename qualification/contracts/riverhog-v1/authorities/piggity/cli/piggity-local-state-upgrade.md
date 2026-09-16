# piggity local state upgrade

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-state-upgrade:14ad5f8329 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-7d7e1764bc"></a>Parser name: `upgrade`
- <a id="s-726c9df3c9"></a>Extra arguments at this parser: rejected.
- <a id="s-ade2ecdd56"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-6cac1d6cb1"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-69cb4766a3"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-ce2a254cb1"></a>`help` | <a id="s-ad416bcd6d"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-08f8597b6f"></a>`0` | <a id="s-a8b2e90643"></a>`"noncontractual-framework-help"` | <a id="s-887b66c58e"></a>`"empty"` |

### Result and failure contract

- <a id="s-a8626cedd1"></a>Result identity: `piggity-cli-result/local/state/upgrade/v1`
- <a id="s-aa92509f2f"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-e905bf6142"></a>Structured output: `optional-json`
- <a id="s-e53a7e2c24"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-bfa86c8e3d"></a>`completed` | <a id="s-d98861536d"></a>`{"kind":"command-completed"}` | <a id="s-7cd3cfb0ef"></a>`0` | <a id="s-ecd561968b"></a>human: `"noncontractual-presentation-of-command-result"`; json: [state-schema-status/v1](#s-9dfef7e7d6) | <a id="s-643eb44d07"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-909b7682b5"></a>`usage` | <a id="s-0d85a756be"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-f28452bff2"></a>`2` | <a id="s-65268670d3"></a>all: `"empty"` | <a id="s-b8b0ebed46"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-ed5d0c67ce"></a>`operational` | <a id="s-c4e7057791"></a>`{"kind":"application-error"}` | <a id="s-74a21eda29"></a>`1` | <a id="s-ee326a54ea"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-cd01b6d2f3"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-9dfef7e7d6"></a>`state-schema-status/v1`

Applies to: completed · stdout (json).

<a id="s-b731fdd709"></a>

- <a id="s-8066a918bd"></a>`type`: `"object"`
- <a id="s-232f035fd6"></a>`additionalProperties`: `false`
- <a id="s-4e18695c91"></a>`required`: `["name","condition","current_revision","head_revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3583b4a660"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-e7b50bd193"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-2e510709c8"></a>`head_revision` | yes | type="string" |  |
| <a id="s-98ba1aa12c"></a>`name` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-69cb4766a3) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-298f823303"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-a59698ede2"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/allow_extra_args`
- `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/name`
- `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/parameters`
- `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/result_contract`
- `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/name`

<!-- exact-contract-value: 192e80d1fe4e27d140b2db67853ff131d7a670e024c440098f759d2df9f2c230 -->

```json
"upgrade"
```

### `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/parameters`

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

### `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/result_contract`

<!-- exact-contract-value: 07ec4f9fcdab42715d4fe2ae5147097971a808d788f84de450d6335b849ac726 -->

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
  "identity": "piggity-cli-result/local/state/upgrade/v1",
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

### `/external_contract/cli/piggity/commands/local/commands/state/commands/upgrade/terminating_controls`

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
