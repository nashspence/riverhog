# a-riverhog-cli collection upload cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-upload-cancel:f644ba0739 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-c371d59b98"></a>Parser name: `cancel`
- <a id="s-d93d82e1c2"></a>Extra arguments at this parser: rejected.
- <a id="s-488d62c540"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-b6ac66f31c"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-1a67a5c96f"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-9c0653bc1b"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-03815832ca"></a>`help` | <a id="s-570aee142f"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-d3028b0227"></a>`0` | <a id="s-207e1010b3"></a>`"noncontractual-framework-help"` | <a id="s-e7e7717b64"></a>`"empty"` |

### Result and failure contract

- <a id="s-3b39b40a1a"></a>Result identity: `a-riverhog-cli-result/collection/upload/cancel/v1`
- <a id="s-5bd82c7ad5"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-a372b9cfe1"></a>Structured output: `optional-json`
- <a id="s-61658e1d69"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-44e67c2000"></a>`completed` | <a id="s-5ec02eca88"></a>`{"kind":"command-completed"}` | <a id="s-93650bfa59"></a>`0` | <a id="s-8a9a36f648"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP cancel_collection_upload_session response 200](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-cancel.md#s-07217e5ae6) | <a id="s-9870ed3044"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6c4dbe1897"></a>`usage` | <a id="s-83d63564df"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-be33eba0ff"></a>`2` | <a id="s-929abd8237"></a>all: `"empty"` | <a id="s-09473f831f"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-60c4fd3fce"></a>`operational` | <a id="s-782719012e"></a>`{"kind":"application-error"}` | <a id="s-6e568206d0"></a>`1` | <a id="s-d4dca64b54"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-1380e5011b"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-1a67a5c96f) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-9c0653bc1b) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/cancel](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-cancel.md)
- [riverhog_client.ApiClient.cancel_collection_upload_session](../../riverhog-client/python/riverhog-client-apiclient-cancel-collection-upload-session.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-8e72c6b848"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-2b6539a16b"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::upload\_cancel\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2408)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/cancel/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/cancel/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/cancel/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/cancel/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/cancel/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/cancel/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/cancel/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/cancel/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/cancel/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/cancel/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/cancel/name`

<!-- exact-contract-value: 5a83111e44703c9dd7431bae2754317bb495994fbf736cf7739842ded4dcbc20 -->

```json
"cancel"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/cancel/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/cancel/result_contract`

<!-- exact-contract-value: 469ab2e6b04c572b064a93a6cd39e38c4699aa3d3ab3321795fc73041f827920 -->

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
  "identity": "a-riverhog-cli-result/collection/upload/cancel/v1",
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
          "operation_id": "cancel_collection_upload_session",
          "path": "/v1/collection-upload-sessions/{collection_id}/cancel",
          "schema": {
            "$ref": "#/components/schemas/CollectionUploadSessionOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/cancel/terminating_controls`

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
