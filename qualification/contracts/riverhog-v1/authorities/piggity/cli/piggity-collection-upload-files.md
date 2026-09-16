# piggity collection upload files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-upload-files:a2e0b9bc88 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-83fe5de5ec"></a>Parser name: `files`
- <a id="s-f62c5ce24d"></a>Extra arguments at this parser: rejected.
- <a id="s-05d5aa13a2"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-9af570c945"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-e11140bd6b"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded |
| <a id="s-c9e3263fee"></a>`page_size`<br>`--page-size` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `25` |
| <a id="s-6eee9ffa9e"></a>`page_token`<br>`--page-token` | optional option; 1 value | text | not recorded |
| <a id="s-f93622962b"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-33d1790b78"></a>`help` | <a id="s-484d68716f"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-2e13ae85c9"></a>`0` | <a id="s-2f005a71af"></a>`"noncontractual-framework-help"` | <a id="s-bd3579076e"></a>`"empty"` |

### Result and failure contract

- <a id="s-8309ce81c8"></a>Result identity: `piggity-cli-result/collection/upload/files/v1`
- <a id="s-097d8608e8"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-adba3bebd2"></a>Structured output: `optional-json`
- <a id="s-658c27d4fa"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-29b84c5bcb"></a>`completed` | <a id="s-0b044fe631"></a>`{"kind":"command-completed"}` | <a id="s-6f0a0ba1db"></a>`0` | <a id="s-e19ffdc2b2"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP list_collection_upload_session_files response 200](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id-files.md#s-4651ddfbd4) | <a id="s-8b6e46d168"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-98e9f05d5e"></a>`usage` | <a id="s-202d7c0504"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-41919d6915"></a>`2` | <a id="s-9bb86a9ea6"></a>all: `empty` | <a id="s-e1e7f98ae1"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-eaab8fba54"></a>`operational` | <a id="s-db12fd8f06"></a>`{"kind":"application-error"}` | <a id="s-486d1db487"></a>`1` | <a id="s-822fb9cf11"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-2a6d94a688"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-e11140bd6b) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-f93622962b) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --page-size](#s-c9e3263fee) | `value · cli-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-c9e3263fee) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-6eee9ffa9e) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}/files](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id-files.md)
- [riverhog_client.ApiClient.list_collection_upload_session_files](../../riverhog-client/python/riverhog-client-apiclient-list-collection-upload-session-files.md)

## Governing policies

- <a id="pa-a23438d51a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-9832ba37fe"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::upload_files_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2375)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/files/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/files/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/files/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/files/name`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/files/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/files/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/files/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/files/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/files/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/files/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/files/name`

<!-- exact-contract-value: 5227e558b8d5353515cf6b89b6766028ca0773ba56a348371c4d61b13d4b3165 -->

```json
"files"
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/files/parameters`

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

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/files/result_contract`

<!-- exact-contract-value: da108548403fe8c87a68bb76eef60c57f5dac7cdd658cce42df557e7d4d294eb -->

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
  "identity": "piggity-cli-result/collection/upload/files/v1",
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
          "method": "GET",
          "operation_id": "list_collection_upload_session_files",
          "path": "/v1/collection-upload-sessions/{collection_id}/files",
          "schema": {
            "$ref": "#/components/schemas/ListCollectionUploadSessionFilesResponse"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/files/terminating_controls`

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
