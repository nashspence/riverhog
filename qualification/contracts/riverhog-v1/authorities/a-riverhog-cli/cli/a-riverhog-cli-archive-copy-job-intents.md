# a-riverhog-cli archive copy-job intents

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-archive-copy-job-intents:cc94b86bb3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-5ff0827018"></a>Parser name: `intents`
- <a id="s-ebf2cd2698"></a>Extra arguments at this parser: rejected.
- <a id="s-db4a1b5b29"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-91e7a86a73"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-3dde4610c8"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-caa3f44e26"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-6ac0865c22"></a>`help` | <a id="s-0323cb1dfa"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-a583ab8cff"></a>`0` | <a id="s-9d6fadfb9a"></a>`"noncontractual-framework-help"` | <a id="s-8d1f25f2cc"></a>`"empty"` |

### Result and failure contract

- <a id="s-8ec7cae5dd"></a>Result identity: `a-riverhog-cli-result/archive/copy-job/intents/v1`
- <a id="s-06e89e89f4"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-1064f2fb5e"></a>Structured output: `optional-json`
- <a id="s-07fae02c4b"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-569e35d7b7"></a>`completed` | <a id="s-cc5f18cc2f"></a>`{"kind":"command-completed"}` | <a id="s-58abeb3de3"></a>`0` | <a id="s-aa410bde07"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_upload_copy_intents response 200](../../riverhog/http-operations/get-v1-archive-upload-copy-intents-collection-id.md#s-584ad596fd) | <a id="s-a90a5adc1c"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f5bc112a9f"></a>`usage` | <a id="s-26077904bb"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-7aa06d4a73"></a>`2` | <a id="s-f22e6be200"></a>all: `"empty"` | <a id="s-033f43a0f4"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-4158bcb341"></a>`operational` | <a id="s-05af2e1432"></a>`{"kind":"application-error"}` | <a id="s-76dce451a2"></a>`1` | <a id="s-5066393b12"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-1b1acc78ff"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-3dde4610c8) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-caa3f44e26) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [GET /v1/archive/upload-copy-intents/{collection_id}](../../riverhog/http-operations/get-v1-archive-upload-copy-intents-collection-id.md)
- [riverhog_client.ApiClient.get_upload_copy_intents](../../riverhog-client/python/riverhog-client-apiclient-get-upload-copy-intents.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-53bf0fde42"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-a7f23920ef"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::archive\_copy\_intents\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L3036)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/intents/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/intents/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/intents/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/intents/name`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/intents/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/intents/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/intents/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/intents/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/intents/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/intents/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/intents/name`

<!-- exact-contract-value: c3b46c60169f9c70ff1c8ceaa5c669340e208fafb25eae2eab0ceba572841f9e -->

```json
"intents"
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/intents/parameters`

<!-- exact-contract-value: 69121b7dd4df39852c314f302ca34fb358e3d4472f9e5565bdec50242e30ee3a -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "collection_id",
    "nargs": 1,
    "options": [
      "collection_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntParamType",
      "name": "integer"
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

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/intents/result_contract`

<!-- exact-contract-value: dcf8feb32d5048065684a20bcf332346b61b07d6fefc41375866f6a945da17e3 -->

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
  "identity": "a-riverhog-cli-result/archive/copy-job/intents/v1",
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
          "operation_id": "get_upload_copy_intents",
          "path": "/v1/archive/upload-copy-intents/{collection_id}",
          "schema": {
            "$ref": "#/components/schemas/UploadCopyIntentsOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/intents/terminating_controls`

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
