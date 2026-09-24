# a-riverhog-cli collection delete

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-delete:4e972b6aa4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-3aafb08941"></a>Parser name: `delete`
- <a id="s-805d0167ac"></a>Extra arguments at this parser: rejected.
- <a id="s-25f8ae841e"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-127eed712a"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-f313017211"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-03eca3cecc"></a>`dry_run`<br>`--dry-run`, `--plan` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-727b565ddf"></a>`confirm`<br>`--confirm` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-97cbb2ede4"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-366649fc92"></a>`help` | <a id="s-60b1c97f7e"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-e7275cbd5d"></a>`0` | <a id="s-41bddfcec6"></a>`"noncontractual-framework-help"` | <a id="s-c0e562f123"></a>`"empty"` |

### Result and failure contract

- <a id="s-1d5c54cffd"></a>Result identity: `a-riverhog-cli-result/collection/delete/v1`
- <a id="s-fc2dada046"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-2618a0ba5f"></a>Structured output: `optional-json`
- <a id="s-114c5dc5fb"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f22c08665c"></a>`planned` | <a id="s-4ef73ce06d"></a>`{"kind":"option-equals","parameter":"dry_run","value":true}` | <a id="s-0753b948e1"></a>`0` | <a id="s-980d36c3e1"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP plan_collection_deletion response 200](../../riverhog/http-operations/post-v1-collections-collection-id-deletion-plan.md#s-c1c9816298) | <a id="s-6cae907f79"></a>all: `"empty"` |
| <a id="s-d5ac62610c"></a>`executed` | <a id="s-3d4cc601ab"></a>`{"kind":"option-equals","parameter":"dry_run","value":false}` | <a id="s-2ed0624e93"></a>`0` | <a id="s-279c1100eb"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP delete_collection response 200](../../riverhog/http-operations/post-v1-collections-collection-id-delete.md#s-257294134c) | <a id="s-f9028e07a5"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-37187d9b6d"></a>`usage` | <a id="s-fd9d286bd8"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-405132d3e5"></a>`2` | <a id="s-d909b70208"></a>all: `"empty"` | <a id="s-de4f443825"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-3594cb69f9"></a>`operational` | <a id="s-4b2bdd5c2a"></a>`{"kind":"application-error"}` | <a id="s-bac5e5cea3"></a>`1` | <a id="s-183882aed3"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-91898424bb"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |
| <a id="s-7e44ee15fd"></a>`blocked` | <a id="s-e79c6cb9d5"></a>`{"kind":"plan-reported-blockers"}` | <a id="s-0016f8b4d3"></a>`1` | <a id="s-a88bfc212f"></a>human: `"noncontractual-presentation-of-command-result"` | <a id="s-a85b207bd7"></a>human: `"empty"` |
| <a id="s-2caa076e61"></a>`confirmation-declined` | <a id="s-ee158c42f4"></a>`{"kind":"interactive-confirmation-mismatch"}` | <a id="s-faee914191"></a>`1` | <a id="s-bcd9b5dfd0"></a>human: `"noncontractual-presentation-of-command-result"` | <a id="s-9527bac7e3"></a>human: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-f313017211) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --confirm](#s-727b565ddf) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --dry-run](#s-03eca3cecc) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --json](#s-97cbb2ede4) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [POST /v1/collections/{collection_id}/delete](../../riverhog/http-operations/post-v1-collections-collection-id-delete.md)
- [POST /v1/collections/{collection_id}/deletion-plan](../../riverhog/http-operations/post-v1-collections-collection-id-deletion-plan.md)
- [riverhog_client.ApiClient.delete_collection](../../riverhog-client/python/riverhog-client-apiclient-delete-collection.md)
- [riverhog_client.ApiClient.plan_collection_deletion](../../riverhog-client/python/riverhog-client-apiclient-plan-collection-deletion.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3cac49046b"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-5e45ba1284"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::collection\_delete\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2926)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/delete/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/delete/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/delete/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/delete/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/delete/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/delete/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/delete/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/delete/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/delete/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/delete/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/delete/name`

<!-- exact-contract-value: b05a18f448a1d2ebeb4812c35e8978201e645044185f821f7511a5c3b62c7e14 -->

```json
"delete"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/delete/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/delete/result_contract`

<!-- exact-contract-value: 941743fe02a0da539384fb9533dca7d0079a9cf7075bc860a5085e7b51379328 -->

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
  "identity": "a-riverhog-cli-result/collection/delete/v1",
  "profile_id": "a-riverhog-cli-human-json/v1",
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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/delete/terminating_controls`

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
