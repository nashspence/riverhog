# a-riverhog-cli app key rotate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-app-key-rotate:d49a4a4882 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-46bf9e0537"></a>Parser name: `rotate`
- <a id="s-35eeca46b4"></a>Extra arguments at this parser: rejected.
- <a id="s-fc3f153e82"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-f67e76aa70"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-9897286298"></a>`app_name`<br>`app_name` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-d584c78e50"></a>`key_id`<br>`key_id` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-52d72f1202"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-2fc90812e0"></a>`help` | <a id="s-481af990cb"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-a4ab942937"></a>`0` | <a id="s-97d578e60f"></a>`"noncontractual-framework-help"` | <a id="s-08b24c5713"></a>`"empty"` |

### Result and failure contract

- <a id="s-7570d7602a"></a>Result identity: `a-riverhog-cli-result/app/key/rotate/v1`
- <a id="s-020309dee8"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-d704446eb8"></a>Structured output: `optional-json`
- <a id="s-8ff8628a98"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-3830051f16"></a>`completed` | <a id="s-4c7ca23c06"></a>`{"kind":"command-completed"}` | <a id="s-c3da244e5c"></a>`0` | <a id="s-27fc789795"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP rotate_app_key response 200](../../riverhog/http-operations/post-v1-apps-app-keys-key-id-rotate.md#s-afcbe75da1) | <a id="s-115193d495"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-61c2f2224d"></a>`usage` | <a id="s-24a32af083"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-0c7478bd91"></a>`2` | <a id="s-1256828ed9"></a>all: `"empty"` | <a id="s-b74ac8faba"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-ead4e78212"></a>`operational` | <a id="s-641333bded"></a>`{"kind":"application-error"}` | <a id="s-83f63ce9ec"></a>`1` | <a id="s-f888ca89df"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-b7efd26576"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter app_name](#s-9897286298) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-52d72f1202) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter key_id](#s-d584c78e50) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys/{key_id}/rotate](../../riverhog/http-operations/post-v1-apps-app-keys-key-id-rotate.md)
- [riverhog_client.ApiClient.rotate_app_key](../../riverhog-client/python/riverhog-client-apiclient-rotate-app-key.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-36c92ac145"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-3679cb859e"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::app\_key\_rotate\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L1223)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/rotate/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/rotate/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/rotate/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/rotate/name`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/rotate/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/rotate/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/rotate/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/rotate/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/rotate/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/rotate/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/rotate/name`

<!-- exact-contract-value: f66ab9323564942157c358fa5caee94868c1433e5c3e672e8456f747813b9a59 -->

```json
"rotate"
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/rotate/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/rotate/result_contract`

<!-- exact-contract-value: b377ef6b51dd39a210e7004092c2965c195a9b1cb5f07b69c59a18ffa38480b2 -->

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
  "identity": "a-riverhog-cli-result/app/key/rotate/v1",
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

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/rotate/terminating_controls`

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
