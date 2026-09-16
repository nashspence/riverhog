# piggity collection upload discard

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-upload-discard:3c2c4478de -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-4b01da3732"></a>Parser name: `discard`
- <a id="s-5fef5096df"></a>Extra arguments at this parser: rejected.
- <a id="s-b451d97117"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-46650ae51a"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-63bbf5885e"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-f375035e7e"></a>`dry_run`<br>`--dry-run`, `--plan` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-9a049cc7bb"></a>`confirm`<br>`--confirm` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-38abeb20f1"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-b56a56efa1"></a>`help` | <a id="s-7178c14f48"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-8b7510ff89"></a>`0` | <a id="s-7807aab912"></a>`"noncontractual-framework-help"` | <a id="s-25bf514780"></a>`"empty"` |

### Result and failure contract

- <a id="s-3a6aab496c"></a>Result identity: `piggity-cli-result/collection/upload/discard/v1`
- <a id="s-f3624faaea"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-0091570fcf"></a>Structured output: `optional-json`
- <a id="s-1f0aef5c08"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-8e890f9515"></a>`planned` | <a id="s-7807825178"></a>`{"kind":"option-equals","parameter":"dry_run","value":true}` | <a id="s-ba00c57c4d"></a>`0` | <a id="s-1efaef072b"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP plan_collection_upload_discard response 200](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-discard-plan.md#s-ee09029c45) | <a id="s-cf43b55869"></a>all: `"empty"` |
| <a id="s-775613b61e"></a>`executed` | <a id="s-5d91af2ea7"></a>`{"kind":"option-equals","parameter":"dry_run","value":false}` | <a id="s-a9abf5e803"></a>`0` | <a id="s-2a4cd02a62"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP discard_collection_upload response 200](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-discard.md#s-e5b9c9f0dc) | <a id="s-ef4bfadd01"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-13699ddc9d"></a>`usage` | <a id="s-0b4f49a657"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-e991784d56"></a>`2` | <a id="s-48f1160a92"></a>all: `"empty"` | <a id="s-e94088cf0a"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-df72a4715c"></a>`operational` | <a id="s-6bc7e75094"></a>`{"kind":"application-error"}` | <a id="s-8eff56bb8d"></a>`1` | <a id="s-d38fcc4ce7"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-029d5235e1"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |
| <a id="s-8e6fdb3179"></a>`blocked` | <a id="s-5231b96fcc"></a>`{"kind":"plan-reported-blockers"}` | <a id="s-64e3bd7a99"></a>`1` | <a id="s-5404debe72"></a>human: `"noncontractual-presentation-of-command-result"` | <a id="s-e12b23c3b3"></a>human: `"empty"` |
| <a id="s-4e3f96da41"></a>`confirmation-declined` | <a id="s-a22f20c789"></a>`{"kind":"interactive-confirmation-mismatch"}` | <a id="s-6cdc9343f1"></a>`1` | <a id="s-46e349a5db"></a>human: `"noncontractual-presentation-of-command-result"` | <a id="s-c77ebaac47"></a>human: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-63bbf5885e) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --confirm](#s-9a049cc7bb) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --dry-run](#s-f375035e7e) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --json](#s-38abeb20f1) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/discard-plan](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-discard-plan.md)
- [POST /v1/collection-upload-sessions/{collection_id}/discard](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-discard.md)
- [riverhog_client.ApiClient.discard_collection_upload](../../riverhog-client/python/riverhog-client-apiclient-discard-collection-upload.md)
- [riverhog_client.ApiClient.plan_collection_upload_discard](../../riverhog-client/python/riverhog-client-apiclient-plan-collection-upload-discard.md)

## Governing policies

- <a id="pa-f7d3f1a5d7"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-acdd6cb7a3"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::upload_discard_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2424)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/name`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/name`

<!-- exact-contract-value: 27e5d8fcb7e7c0c194453fff8dcce54dbd2cec00c0d18c995023ce62b981b7da -->

```json
"discard"
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/parameters`

<!-- exact-contract-value: e6bd272eae2fa7e8f7abcba6ea104a2fada51f1e99c3ea07bb15c66d70403834 -->

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
    "name": "dry_run",
    "nargs": 1,
    "options": [
      "--dry-run",
      "--plan"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "confirm",
    "nargs": 1,
    "options": [
      "--confirm"
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

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/result_contract`

<!-- exact-contract-value: 6b4dd34fee425578036f4d45a50b4e73b88926d2ae952cf1a03672d7c6025dd0 -->

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
    },
    {
      "exit_status": 1,
      "id": "blocked",
      "selected_by": {
        "kind": "plan-reported-blockers"
      },
      "stderr": {
        "human": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result"
      }
    },
    {
      "exit_status": 1,
      "id": "confirmation-declined",
      "selected_by": {
        "kind": "interactive-confirmation-mismatch"
      },
      "stderr": {
        "human": "noncontractual-diagnostic"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "piggity-cli-result/collection/upload/discard/v1",
  "profile_id": "piggity-cli-human-json/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "planned",
      "selected_by": {
        "kind": "option-equals",
        "parameter": "dry_run",
        "value": true
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
          "operation_id": "plan_collection_upload_discard",
          "path": "/v1/collection-upload-sessions/{collection_id}/discard-plan",
          "schema": {
            "$ref": "#/components/schemas/CollectionUploadDiscardPlanOut"
          },
          "status": "200"
        }
      }
    },
    {
      "exit_status": 0,
      "id": "executed",
      "selected_by": {
        "kind": "option-equals",
        "parameter": "dry_run",
        "value": false
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
          "operation_id": "discard_collection_upload",
          "path": "/v1/collection-upload-sessions/{collection_id}/discard",
          "schema": {
            "$ref": "#/components/schemas/CollectionUploadDiscardResultOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/discard/terminating_controls`

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
