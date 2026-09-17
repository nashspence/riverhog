# piggity collection delete

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-delete:372c3b7727 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-accbc5cbe6"></a>Parser name: `delete`
- <a id="s-796b75f56e"></a>Extra arguments at this parser: rejected.
- <a id="s-f5cef58f29"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-5172810072"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-d6934e56de"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-105e737284"></a>`dry_run`<br>`--dry-run`, `--plan` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-0b1ae3ab19"></a>`confirm`<br>`--confirm` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-b0369c7ed6"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-aebbdefcf9"></a>`help` | <a id="s-745fdf5a60"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-26269bf8af"></a>`0` | <a id="s-2486acddac"></a>`"noncontractual-framework-help"` | <a id="s-63ed1d333d"></a>`"empty"` |

### Result and failure contract

- <a id="s-c3225b5395"></a>Result identity: `piggity-cli-result/collection/delete/v1`
- <a id="s-3ae6133e67"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-85170dacda"></a>Structured output: `optional-json`
- <a id="s-efa45087ad"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a3853a44a8"></a>`planned` | <a id="s-04c5097556"></a>`{"kind":"option-equals","parameter":"dry_run","value":true}` | <a id="s-88a93d4afa"></a>`0` | <a id="s-ccf18d3d82"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP plan_collection_deletion response 200](../../riverhog/http-operations/post-v1-collections-collection-id-deletion-plan.md#s-c1c9816298) | <a id="s-c2db8e784e"></a>all: `"empty"` |
| <a id="s-147fd57fd4"></a>`executed` | <a id="s-e36a4a5930"></a>`{"kind":"option-equals","parameter":"dry_run","value":false}` | <a id="s-dd198b4019"></a>`0` | <a id="s-902f93111d"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP delete_collection response 200](../../riverhog/http-operations/post-v1-collections-collection-id-delete.md#s-257294134c) | <a id="s-081438477c"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f5da864260"></a>`usage` | <a id="s-dd23f76561"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-82187dd7f7"></a>`2` | <a id="s-33e0e19560"></a>all: `"empty"` | <a id="s-50bc038faf"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-ded7ed275f"></a>`operational` | <a id="s-782b3de2a1"></a>`{"kind":"application-error"}` | <a id="s-d4e64b1c68"></a>`1` | <a id="s-47e6b6df91"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-651a9a011d"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |
| <a id="s-3e686b6e9b"></a>`blocked` | <a id="s-d937144599"></a>`{"kind":"plan-reported-blockers"}` | <a id="s-e9870664eb"></a>`1` | <a id="s-12f27115d1"></a>human: `"noncontractual-presentation-of-command-result"` | <a id="s-4e8913613a"></a>human: `"empty"` |
| <a id="s-7f6714b433"></a>`confirmation-declined` | <a id="s-c410f09696"></a>`{"kind":"interactive-confirmation-mismatch"}` | <a id="s-c5a8fbb6f5"></a>`1` | <a id="s-affc944b60"></a>human: `"noncontractual-presentation-of-command-result"` | <a id="s-d10a6cff56"></a>human: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-d6934e56de) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --confirm](#s-0b1ae3ab19) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --dry-run](#s-105e737284) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --json](#s-b0369c7ed6) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [POST /v1/collections/{collection_id}/delete](../../riverhog/http-operations/post-v1-collections-collection-id-delete.md)
- [POST /v1/collections/{collection_id}/deletion-plan](../../riverhog/http-operations/post-v1-collections-collection-id-deletion-plan.md)
- [riverhog_client.ApiClient.delete_collection](../../riverhog-client/python/riverhog-client-apiclient-delete-collection.md)
- [riverhog_client.ApiClient.plan_collection_deletion](../../riverhog-client/python/riverhog-client-apiclient-plan-collection-deletion.md)

## Governing policies

- <a id="pa-4850efa6f3"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-e342a46eed"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::collection\_delete\_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2911)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/delete/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/delete/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/delete/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/delete/name`
- `/external_contract/cli/piggity/commands/collection/commands/delete/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/delete/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/delete/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/delete/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/delete/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/delete/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/delete/name`

<!-- exact-contract-value: b05a18f448a1d2ebeb4812c35e8978201e645044185f821f7511a5c3b62c7e14 -->

```json
"delete"
```

### `/external_contract/cli/piggity/commands/collection/commands/delete/parameters`

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

### `/external_contract/cli/piggity/commands/collection/commands/delete/result_contract`

<!-- exact-contract-value: a04cd45ca0d7cb03dd76787d94d9b3e38e083833ae6012ca2782340438a0c034 -->

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
  "identity": "piggity-cli-result/collection/delete/v1",
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
          "operation_id": "plan_collection_deletion",
          "path": "/v1/collections/{collection_id}/deletion-plan",
          "schema": {
            "$ref": "#/components/schemas/CollectionDeletionPlanOut"
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
          "operation_id": "delete_collection",
          "path": "/v1/collections/{collection_id}/delete",
          "schema": {
            "$ref": "#/components/schemas/CollectionDeletionResultOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/delete/terminating_controls`

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
