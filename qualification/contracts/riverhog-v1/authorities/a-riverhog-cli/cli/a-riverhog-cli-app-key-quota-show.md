# a-riverhog-cli app key quota show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-app-key-quota-show:191d5d3402 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-ac5cb343e8"></a>Parser name: `show`
- <a id="s-0c4f10fb32"></a>Extra arguments at this parser: rejected.
- <a id="s-5fb5cf7a9c"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-395fdb0879"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-e16fe2ef14"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-d16391ed3e"></a>`help` | <a id="s-397e4fc36d"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-075dbc306f"></a>`0` | <a id="s-fb4d085b47"></a>`"noncontractual-framework-help"` | <a id="s-dba74f6bb0"></a>`"empty"` |

### Result and failure contract

- <a id="s-8430a0a45b"></a>Result identity: `a-riverhog-cli-result/app/key/quota/show/v1`
- <a id="s-e8294e1972"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-8acc2093d6"></a>Structured output: `optional-json`
- <a id="s-d2d6230498"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-8f3a6b749d"></a>`completed` | <a id="s-e7c607922f"></a>`{"kind":"command-completed"}` | <a id="s-4024b30900"></a>`0` | <a id="s-561ffe85f4"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_download_quota response 200](../../riverhog/http-operations/get-v1-download-quota.md#s-0b2951931e) | <a id="s-68fdc9b2aa"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-690e0bc269"></a>`usage` | <a id="s-88ef7576c6"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-8a4e2b0182"></a>`2` | <a id="s-c47bf309eb"></a>all: `"empty"` | <a id="s-a394597f8e"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-ef62fdd78e"></a>`operational` | <a id="s-5a2cf7a162"></a>`{"kind":"application-error"}` | <a id="s-01a9df0f6a"></a>`1` | <a id="s-785a149d3a"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-62d4d2a5eb"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-e16fe2ef14) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/download-quota](../../riverhog/http-operations/get-v1-download-quota.md)
- [riverhog_client.ApiClient.get_download_quota](../../riverhog-client/python/riverhog-client-apiclient-get-download-quota.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-423557334f"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-3b354bf575"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::app\_key\_quota\_show\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L1357)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/show/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/show/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/show/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/show/name`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/show/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/show/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/show/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/show/result_contract`

<!-- exact-contract-value: 4508cf5124a42dd899fd7b1c33592dff70d6e58247310ed25f3fec7367bf25a4 -->

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
  "identity": "a-riverhog-cli-result/app/key/quota/show/v1",
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
          "application": "riverhog",
          "kind": "http-operation-response",
          "method": "GET",
          "operation_id": "get_download_quota",
          "path": "/v1/download-quota",
          "schema": {
            "$ref": "#/components/schemas/KeyDownloadQuotaOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/show/terminating_controls`

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
