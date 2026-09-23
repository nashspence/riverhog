# a-riverhog-cli collection upload watch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-upload-watch:afcf4e2319 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-71e780d211"></a>Parser name: `watch`
- <a id="s-d5cdceb324"></a>Extra arguments at this parser: rejected.
- <a id="s-f3d6df81f5"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-23ae060d58"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-8931fcad0f"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-ab50be2a5f"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-d933223f62"></a>`help` | <a id="s-1a23530a77"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-24ab232221"></a>`0` | <a id="s-4d22b5ea08"></a>`"noncontractual-framework-help"` | <a id="s-88816e5980"></a>`"empty"` |

### Result and failure contract

- <a id="s-4bbf0ed655"></a>Result identity: `a-riverhog-cli-result/collection/upload/watch/v1`
- <a id="s-2f2e2c6a65"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-aa4ffc0614"></a>Structured output: `optional-json`
- <a id="s-447c8d09eb"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1bcc1a64ec"></a>`completed` | <a id="s-7478ea42be"></a>`{"kind":"command-completed"}` | <a id="s-5e32b08b65"></a>`0` | <a id="s-7daa552dd0"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_collection_upload_session response 200](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id.md#s-3478a57d08) | <a id="s-4648075eb0"></a>all: `"noncontractual-progress"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-3888e1db98"></a>`usage` | <a id="s-4b060b6233"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-5550da5bc1"></a>`2` | <a id="s-e8eb21cbb5"></a>all: `"empty"` | <a id="s-dc42aaec11"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-70ba32cfcd"></a>`operational` | <a id="s-11f1f74f39"></a>`{"kind":"application-error"}` | <a id="s-7732721249"></a>`1` | <a id="s-114e04b101"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-0214cf421a"></a>human: `"noncontractual-diagnostic-or-progress"`; json: `"noncontractual-progress"` |
| <a id="s-b9485ac8c9"></a>`custody-timeout` | <a id="s-458f011cd7"></a>`{"kind":"custody-deadline-expired","state":"not-finalized"}` | <a id="s-bada2a72a8"></a>`124` | <a id="s-6d0ab1269f"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_collection_upload_session response 200](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id.md#s-3478a57d08) | <a id="s-4c1865e491"></a>all: `"noncontractual-progress"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-8931fcad0f) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-ab50be2a5f) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id.md)
- [riverhog_client.ApiClient.get_collection_upload_session](../../riverhog-client/python/riverhog-client-apiclient-get-collection-upload-session.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-395ee553b3"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-be9b873fe9"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::upload\_watch\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2419)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/watch/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/watch/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/watch/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/watch/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/watch/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/watch/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/watch/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/watch/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/watch/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/watch/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/watch/name`

<!-- exact-contract-value: 73da76bff71a604995ddd94e223ffa8b7c171b54e0a953c0fb794ac85a61534b -->

```json
"watch"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/watch/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/watch/result_contract`

<!-- exact-contract-value: 0b808ef510f2adaa9f13f1c5dd88b63d98052753ec0193258dbf6029fa057eb6 -->

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
        "human": "noncontractual-diagnostic-or-progress",
        "json": "noncontractual-progress"
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
      "exit_status": 124,
      "id": "custody-timeout",
      "selected_by": {
        "kind": "custody-deadline-expired",
        "state": "not-finalized"
      },
      "stderr": {
        "all": "noncontractual-progress"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "application": "riverhog",
          "kind": "http-operation-response",
          "method": "GET",
          "operation_id": "get_collection_upload_session",
          "path": "/v1/collection-upload-sessions/{collection_id}",
          "schema": {
            "$ref": "#/components/schemas/CollectionUploadSessionOut"
          },
          "status": "200"
        }
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "a-riverhog-cli-result/collection/upload/watch/v1",
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
        "all": "noncontractual-progress"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "application": "riverhog",
          "kind": "http-operation-response",
          "method": "GET",
          "operation_id": "get_collection_upload_session",
          "path": "/v1/collection-upload-sessions/{collection_id}",
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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/watch/terminating_controls`

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
