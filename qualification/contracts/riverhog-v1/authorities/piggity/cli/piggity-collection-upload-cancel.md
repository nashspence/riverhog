# piggity collection upload cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-upload-cancel:3d4a9ca04f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-0bb25d44e5"></a>Parser name: `cancel`
- <a id="s-6fa3bdba0e"></a>Extra arguments at this parser: rejected.
- <a id="s-846845f9bf"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-fa0f2558ca"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-582579864a"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-d35c5ac384"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-9a1f83b391"></a>`help` | <a id="s-2923f931ac"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-3537b8bc71"></a>`0` | <a id="s-4da5078d7a"></a>`"noncontractual-framework-help"` | <a id="s-75302a74c2"></a>`"empty"` |

### Result and failure contract

- <a id="s-c19799abfd"></a>Result identity: `piggity-cli-result/collection/upload/cancel/v1`
- <a id="s-9be70bcbd1"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-2e985ee5cc"></a>Structured output: `optional-json`
- <a id="s-29e08348ef"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f76a96ae14"></a>`completed` | <a id="s-41c8d8b7ba"></a>`{"kind":"command-completed"}` | <a id="s-ef04785b3a"></a>`0` | <a id="s-527fa41162"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP cancel_collection_upload_session response 200](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-cancel.md#s-07217e5ae6) | <a id="s-a16f039710"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-87bb744a95"></a>`usage` | <a id="s-54e56b6f54"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-c6c866285a"></a>`2` | <a id="s-ba8542af27"></a>all: `"empty"` | <a id="s-19c5c58ef4"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-393e149b14"></a>`operational` | <a id="s-2e981dd78c"></a>`{"kind":"application-error"}` | <a id="s-7b90a7af3e"></a>`1` | <a id="s-6271841b18"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-f9106453e8"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-582579864a) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-d35c5ac384) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/cancel](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-cancel.md)
- [riverhog_client.ApiClient.cancel_collection_upload_session](../../riverhog-client/python/riverhog-client-apiclient-cancel-collection-upload-session.md)

## Governing policies

- <a id="pa-11a502a900"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-0b412bf69f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::upload\_cancel\_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2393)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/cancel/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/cancel/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/cancel/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/cancel/name`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/cancel/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/cancel/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/cancel/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/cancel/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/cancel/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/cancel/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/cancel/name`

<!-- exact-contract-value: 5a83111e44703c9dd7431bae2754317bb495994fbf736cf7739842ded4dcbc20 -->

```json
"cancel"
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/cancel/parameters`

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

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/cancel/result_contract`

<!-- exact-contract-value: 6fe21af125003818d59ccbf49bcaade2ffc7ec579d1a38fea44eee118c243fef -->

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
  "identity": "piggity-cli-result/collection/upload/cancel/v1",
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

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/cancel/terminating_controls`

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
