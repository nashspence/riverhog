# piggity app key rotate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key-rotate:fd69a860e8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-383824b169"></a>Parser name: `rotate`
- <a id="s-d75cfd3068"></a>Extra arguments at this parser: rejected.
- <a id="s-30d20a88d3"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-b4d53c59ff"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-667bdb79a0"></a>`app_name`<br>`app_name` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-fa35dffec2"></a>`key_id`<br>`key_id` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-6529995c44"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-6aba098818"></a>`help` | <a id="s-eee4d1e758"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-76ede61180"></a>`0` | <a id="s-0795b3748c"></a>`"noncontractual-framework-help"` | <a id="s-96b139cd20"></a>`"empty"` |

### Result and failure contract

- <a id="s-a6b465fea1"></a>Result identity: `piggity-cli-result/app/key/rotate/v1`
- <a id="s-5888ab4ed9"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-875de54892"></a>Structured output: `optional-json`
- <a id="s-56f297383f"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-fb5f9c867f"></a>`completed` | <a id="s-a844de8be0"></a>`{"kind":"command-completed"}` | <a id="s-c2d6dfd70a"></a>`0` | <a id="s-641a48ee38"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP rotate_app_key response 200](../../riverhog/http-operations/post-v1-apps-app-keys-key-id-rotate.md#s-afcbe75da1) | <a id="s-79e7c04085"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a0d9f03ecd"></a>`usage` | <a id="s-9c63676872"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-e126783be0"></a>`2` | <a id="s-9d3973c593"></a>all: `"empty"` | <a id="s-fafdf928b0"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-4478e2c0c0"></a>`operational` | <a id="s-46b464e97f"></a>`{"kind":"application-error"}` | <a id="s-08ba5101a1"></a>`1` | <a id="s-c2fee5a1ca"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-13e3b07f3c"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter app_name](#s-667bdb79a0) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-6529995c44) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter key_id](#s-fa35dffec2) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys/{key_id}/rotate](../../riverhog/http-operations/post-v1-apps-app-keys-key-id-rotate.md)
- [riverhog_client.ApiClient.rotate_app_key](../../riverhog-client/python/riverhog-client-apiclient-rotate-app-key.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3a0600115f"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-84298f2d16"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources/authorities.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::app\_key\_rotate\_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L1221)

### Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/allow_extra_args`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/name`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/parameters`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/result_contract`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/name`

<!-- exact-contract-value: f66ab9323564942157c358fa5caee94868c1433e5c3e672e8456f747813b9a59 -->

```json
"rotate"
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/parameters`

<!-- exact-contract-value: 1749f130449a17b41843bc114da325f40e7d6ade60854043985e5bcec11d2cad -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "app_name",
    "nargs": 1,
    "options": [
      "app_name"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "key_id",
    "nargs": 1,
    "options": [
      "key_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
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

### `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/result_contract`

<!-- exact-contract-value: b7cdf3e674e7aa652326af0a5146ffc5e718c590c0d47b22fa9d509405f7f26f -->

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
  "identity": "piggity-cli-result/app/key/rotate/v1",
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
          "application": "riverhog",
          "kind": "http-operation-response",
          "method": "POST",
          "operation_id": "rotate_app_key",
          "path": "/v1/apps/{app}/keys/{key_id}/rotate",
          "schema": {
            "$ref": "#/components/schemas/AppKeyCreatedOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/rotate/terminating_controls`

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
