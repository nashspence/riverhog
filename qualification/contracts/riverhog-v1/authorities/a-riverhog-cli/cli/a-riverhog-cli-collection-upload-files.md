# a-riverhog-cli collection upload files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-upload-files:49d23e4341 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-ff2301e89f"></a>Parser name: `files`
- <a id="s-adebe9e86c"></a>Extra arguments at this parser: rejected.
- <a id="s-052a686497"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-574af232e0"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-766c092669"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-92e80109ea"></a>`page_size`<br>`--page-size` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `25`<br>Env: `null` |
| <a id="s-c678fe39f0"></a>`page_token`<br>`--page-token` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-a633977d7e"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-65e761f93d"></a>`help` | <a id="s-395e410cb1"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-bbb34e3a42"></a>`0` | <a id="s-627bdcfd29"></a>`"noncontractual-framework-help"` | <a id="s-527ae53d0b"></a>`"empty"` |

### Result and failure contract

- <a id="s-82f4e4df16"></a>Result identity: `a-riverhog-cli-result/collection/upload/files/v1`
- <a id="s-6e77075f2d"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-b09c91d44b"></a>Structured output: `optional-json`
- <a id="s-94fbbd4088"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-7116688d44"></a>`completed` | <a id="s-0a912370b1"></a>`{"kind":"command-completed"}` | <a id="s-2e4b227d67"></a>`0` | <a id="s-add4c5290a"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP list_collection_upload_session_files response 200](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id-files.md#s-4651ddfbd4) | <a id="s-96d1e68238"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-9e569705a5"></a>`usage` | <a id="s-52df9ab7ff"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-72cded8fcc"></a>`2` | <a id="s-f9febca1b0"></a>all: `"empty"` | <a id="s-290b2d9e4c"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-e0ec675065"></a>`operational` | <a id="s-0d22479a4a"></a>`{"kind":"application-error"}` | <a id="s-b7ea4a5525"></a>`1` | <a id="s-33ce7bf21c"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-7f581ff690"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-766c092669) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-a633977d7e) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --page-size](#s-92e80109ea) | `value · cli-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-92e80109ea) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-c678fe39f0) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}/files](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id-files.md)
- [riverhog_client.ApiClient.list_collection_upload_session_files](../../riverhog-client/python/riverhog-client-apiclient-list-collection-upload-session-files.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-07f3e57193"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-974f9cee93"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::upload\_files\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2414)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/files/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/files/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/files/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/files/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/files/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/files/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/files/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/files/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/files/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/files/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/files/name`

<!-- exact-contract-value: 5227e558b8d5353515cf6b89b6766028ca0773ba56a348371c4d61b13d4b3165 -->

```json
"files"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/files/parameters`

<!-- exact-contract-value: 7b3227fc02affb8ca13825e20a9df9fcd88cafb88ff278e9561b3eeeb79d7017 -->

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
    "default": 25,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "page_size",
    "nargs": 1,
    "options": [
      "--page-size"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "clamp": false,
      "class": "typer._click.types.IntRange",
      "max_open": false,
      "maximum": 100,
      "min_open": false,
      "minimum": 1,
      "name": "integer range"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "page_token",
    "nargs": 1,
    "options": [
      "--page-token"
    ],
    "required": false,
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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/files/result_contract`

<!-- exact-contract-value: 7399c85079d11536ddb835e9732ddc54963702bff8fe9216cb81f6106169b3f6 -->

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
  "identity": "a-riverhog-cli-result/collection/upload/files/v1",
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
          "operation_id": "list_collection_upload_session_files",
          "path": "/v1/collection-upload-sessions/{collection_id}/files",
          "schema": {
            "$ref": "#/components/schemas/ListCollectionUploadSessionFilesOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/files/terminating_controls`

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
