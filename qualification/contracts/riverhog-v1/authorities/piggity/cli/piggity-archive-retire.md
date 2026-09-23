# piggity archive retire

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-archive-retire:d522453967 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-e5a72fb08f"></a>Parser name: `retire`
- <a id="s-ec72008766"></a>Extra arguments at this parser: rejected.
- <a id="s-d14cc8b359"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-784c590676"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-74ff7f0ecb"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-1cad34944b"></a>`store`<br>`--store` | required option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-6334a27c26"></a>`dry_run`<br>`--dry-run`, `--plan` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-bc3027a32b"></a>`confirm`<br>`--confirm` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-bb4c28ef4b"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-dce607cc58"></a>`help` | <a id="s-f6b62f77e5"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-adf0b33750"></a>`0` | <a id="s-2feea014ef"></a>`"noncontractual-framework-help"` | <a id="s-48ae3289cd"></a>`"empty"` |

### Result and failure contract

- <a id="s-f7b7d91e17"></a>Result identity: `piggity-cli-result/archive/retire/v1`
- <a id="s-a76ad1c0ea"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-f0a299c8ac"></a>Structured output: `optional-json`
- <a id="s-e6bb996387"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-99895d5327"></a>`planned` | <a id="s-64a4de99c4"></a>`{"kind":"option-equals","parameter":"dry_run","value":true}` | <a id="s-679543bbd6"></a>`0` | <a id="s-755e242947"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP plan_archive_copy_retirement response 200](../../riverhog/http-operations/post-v1-archive-copies-retirement-plan.md#s-75066bfe2a) | <a id="s-2f630eeedf"></a>all: `"empty"` |
| <a id="s-bd985cbae6"></a>`executed` | <a id="s-d18be48412"></a>`{"kind":"option-equals","parameter":"dry_run","value":false}` | <a id="s-226a0ac440"></a>`0` | <a id="s-a5acf1c320"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP retire_archive_copy response 200](../../riverhog/http-operations/post-v1-archive-copies-retire.md#s-727ed61cf8) | <a id="s-6df3b708e4"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-8910a67b1f"></a>`usage` | <a id="s-2081e9c272"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-a159384c27"></a>`2` | <a id="s-71881e206f"></a>all: `"empty"` | <a id="s-e56b218030"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-8b235c6ce0"></a>`operational` | <a id="s-19506b04c0"></a>`{"kind":"application-error"}` | <a id="s-29c43e7cf1"></a>`1` | <a id="s-95921aba6c"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-a89dbe5a9b"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |
| <a id="s-8a42e3889d"></a>`blocked` | <a id="s-da32c7e454"></a>`{"kind":"plan-reported-blockers"}` | <a id="s-c8010cb636"></a>`1` | <a id="s-d0711626b5"></a>human: `"noncontractual-presentation-of-command-result"` | <a id="s-47bee7eb9a"></a>human: `"empty"` |
| <a id="s-f0b56a096d"></a>`confirmation-declined` | <a id="s-c156fe737f"></a>`{"kind":"interactive-confirmation-mismatch"}` | <a id="s-a10a6558d9"></a>`1` | <a id="s-bf862a00be"></a>human: `"noncontractual-presentation-of-command-result"` | <a id="s-df55cf1650"></a>human: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-74ff7f0ecb) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --confirm](#s-bc3027a32b) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --dry-run](#s-6334a27c26) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --json](#s-bb4c28ef4b) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --store](#s-1cad34944b) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [POST /v1/archive/copies/retire](../../riverhog/http-operations/post-v1-archive-copies-retire.md)
- [POST /v1/archive/copies/retirement-plan](../../riverhog/http-operations/post-v1-archive-copies-retirement-plan.md)
- [riverhog_client.ApiClient.plan_archive_copy_retirement](../../riverhog-client/python/riverhog-client-apiclient-plan-archive-copy-retirement.md)
- [riverhog_client.ApiClient.retire_archive_copy](../../riverhog-client/python/riverhog-client-apiclient-retire-archive-copy.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-0297f57ebb"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-bba0b7a032"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources/authorities.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::archive\_retire\_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L3110)

### Machine authority

- `/external_contract/cli/piggity/commands/archive/commands/retire/allow_extra_args`
- `/external_contract/cli/piggity/commands/archive/commands/retire/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/archive/commands/retire/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/archive/commands/retire/name`
- `/external_contract/cli/piggity/commands/archive/commands/retire/parameters`
- `/external_contract/cli/piggity/commands/archive/commands/retire/result_contract`
- `/external_contract/cli/piggity/commands/archive/commands/retire/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/archive/commands/retire/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/archive/commands/retire/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/archive/commands/retire/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/archive/commands/retire/name`

<!-- exact-contract-value: a2a9d4ffbc4c361801bc55cbc97e993b34974d1cb94ba86138a02d7b5630ad91 -->

```json
"retire"
```

### `/external_contract/cli/piggity/commands/archive/commands/retire/parameters`

<!-- exact-contract-value: 116b926952db53e8cab7ae68dd815774c23383fb5dd194c74c89118b771e12fa -->

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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "store",
    "nargs": 1,
    "options": [
      "--store"
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

### `/external_contract/cli/piggity/commands/archive/commands/retire/result_contract`

<!-- exact-contract-value: 906b540248de0bfad7e3d1127d0e123f63e9c5749b750e99903bea44ac853a2c -->

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
  "identity": "piggity-cli-result/archive/retire/v1",
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
          "operation_id": "plan_archive_copy_retirement",
          "path": "/v1/archive/copies/retirement-plan",
          "schema": {
            "$ref": "#/components/schemas/ArchiveCopyRetirementPlanOut"
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
          "operation_id": "retire_archive_copy",
          "path": "/v1/archive/copies/retire",
          "schema": {
            "$ref": "#/components/schemas/ArchiveCopyRetirementResultOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/archive/commands/retire/terminating_controls`

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
