# a-riverhog-cli archive retire

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-archive-retire:0ed3c6052f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-c06dee4c5c"></a>Parser name: `retire`
- <a id="s-259f1f3232"></a>Extra arguments at this parser: rejected.
- <a id="s-cc90fac3c3"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-0aaeb59c66"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-666c122f61"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-bf96ed3b29"></a>`store`<br>`--store` | required option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-9a96caf099"></a>`dry_run`<br>`--dry-run`, `--plan` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-551c499674"></a>`confirm`<br>`--confirm` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-efaa53e286"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-67a6371eed"></a>`help` | <a id="s-c8f2d9723c"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-fbaef5d84f"></a>`0` | <a id="s-eaf54f9e12"></a>`"noncontractual-framework-help"` | <a id="s-4c7169d66e"></a>`"empty"` |

### Result and failure contract

- <a id="s-5a653c7aa1"></a>Result identity: `a-riverhog-cli-result/archive/retire/v1`
- <a id="s-3704f8ed93"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-6d14d86825"></a>Structured output: `optional-json`
- <a id="s-b1f67a9145"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-fa6610efa8"></a>`planned` | <a id="s-16dc0f3ef6"></a>`{"kind":"option-equals","parameter":"dry_run","value":true}` | <a id="s-835a96fa50"></a>`0` | <a id="s-58a96b107e"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP plan_archive_copy_retirement response 200](../../riverhog/http-operations/post-v1-archive-copies-retirement-plan.md#s-75066bfe2a) | <a id="s-fb5d70fbb9"></a>all: `"empty"` |
| <a id="s-e4e84f2d7e"></a>`executed` | <a id="s-3a6d7a022d"></a>`{"kind":"option-equals","parameter":"dry_run","value":false}` | <a id="s-09ed9394fc"></a>`0` | <a id="s-84ca534197"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP retire_archive_copy response 200](../../riverhog/http-operations/post-v1-archive-copies-retire.md#s-727ed61cf8) | <a id="s-2e47f114e6"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-d51849280f"></a>`usage` | <a id="s-3846a86e54"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-1d0475733d"></a>`2` | <a id="s-5ef26c197b"></a>all: `"empty"` | <a id="s-15b662c01f"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-28614c358b"></a>`operational` | <a id="s-2bcadd5a95"></a>`{"kind":"application-error"}` | <a id="s-029dd44c93"></a>`1` | <a id="s-b0c1089312"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-1a75f700b1"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |
| <a id="s-15a4b22f00"></a>`blocked` | <a id="s-1e6bdd8d7a"></a>`{"kind":"plan-reported-blockers"}` | <a id="s-37156dba16"></a>`1` | <a id="s-d234b22f48"></a>human: `"noncontractual-presentation-of-command-result"` | <a id="s-aa3b467e20"></a>human: `"empty"` |
| <a id="s-21a720931e"></a>`confirmation-declined` | <a id="s-5587d42d23"></a>`{"kind":"interactive-confirmation-mismatch"}` | <a id="s-937e0f9d24"></a>`1` | <a id="s-4d86a27db3"></a>human: `"noncontractual-presentation-of-command-result"` | <a id="s-c1631540e7"></a>human: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-666c122f61) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --confirm](#s-551c499674) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --dry-run](#s-9a96caf099) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --json](#s-efaa53e286) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --store](#s-bf96ed3b29) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [POST /v1/archive/copies/retire](../../riverhog/http-operations/post-v1-archive-copies-retire.md)
- [POST /v1/archive/copies/retirement-plan](../../riverhog/http-operations/post-v1-archive-copies-retirement-plan.md)
- [riverhog_client.ApiClient.plan_archive_copy_retirement](../../riverhog-client/python/riverhog-client-apiclient-plan-archive-copy-retirement.md)
- [riverhog_client.ApiClient.retire_archive_copy](../../riverhog-client/python/riverhog-client-apiclient-retire-archive-copy.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-a742ad9c52"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-1ea6665832"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::archive\_retire\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L3180)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/retire/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/retire/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/retire/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/retire/name`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/retire/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/retire/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/retire/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/retire/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/retire/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/retire/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/retire/name`

<!-- exact-contract-value: a2a9d4ffbc4c361801bc55cbc97e993b34974d1cb94ba86138a02d7b5630ad91 -->

```json
"retire"
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/retire/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/retire/result_contract`

<!-- exact-contract-value: 42e5c4cfce252cc9635524db47ce48866b9f8707a257ee15275fddf1414f2e8d -->

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
  "identity": "a-riverhog-cli-result/archive/retire/v1",
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

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/retire/terminating_controls`

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
