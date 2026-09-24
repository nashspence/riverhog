# a-riverhog-cli collection upload discard

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-upload-discard:968141fe3f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-2b772c5180"></a>Parser name: `discard`
- <a id="s-26233d5261"></a>Extra arguments at this parser: rejected.
- <a id="s-adf4806d4e"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-7ea2b1a8c6"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-2d7e5e454c"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-164339ed67"></a>`dry_run`<br>`--dry-run`, `--plan` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-64685d54c7"></a>`confirm`<br>`--confirm` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-b8eaec0e8f"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-4e6ae418d6"></a>`help` | <a id="s-14673bc457"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-8fc4a6a119"></a>`0` | <a id="s-6136c14e22"></a>`"noncontractual-framework-help"` | <a id="s-ee712b7d43"></a>`"empty"` |

### Result and failure contract

- <a id="s-1005b24364"></a>Result identity: `a-riverhog-cli-result/collection/upload/discard/v1`
- <a id="s-8c0bd74148"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-91bbcb6745"></a>Structured output: `optional-json`
- <a id="s-108ec2c3f7"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-bdfbb05b8e"></a>`planned` | <a id="s-1c74e162bc"></a>`{"kind":"option-equals","parameter":"dry_run","value":true}` | <a id="s-69e9411e6b"></a>`0` | <a id="s-60b0c121eb"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP plan_collection_upload_discard response 200](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-discard-plan.md#s-ee09029c45) | <a id="s-33c2c86efa"></a>all: `"empty"` |
| <a id="s-e03b6a47e2"></a>`executed` | <a id="s-c67f66a0f6"></a>`{"kind":"option-equals","parameter":"dry_run","value":false}` | <a id="s-1a385e4b31"></a>`0` | <a id="s-6c2c829783"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP discard_collection_upload response 200](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-discard.md#s-e5b9c9f0dc) | <a id="s-67b1dc6bd9"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-7f1ed6dd33"></a>`usage` | <a id="s-95f1755026"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-1a3bf8ad61"></a>`2` | <a id="s-7731ed346d"></a>all: `"empty"` | <a id="s-0861b51bc5"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-494f0bb3a1"></a>`operational` | <a id="s-ef04863ace"></a>`{"kind":"application-error"}` | <a id="s-0ebf268d92"></a>`1` | <a id="s-2896b866a5"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-fa71c908d5"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |
| <a id="s-db17ffe490"></a>`blocked` | <a id="s-3b87ccad44"></a>`{"kind":"plan-reported-blockers"}` | <a id="s-bc8bbe60db"></a>`1` | <a id="s-1863eafdc2"></a>human: `"noncontractual-presentation-of-command-result"` | <a id="s-a65a57edf9"></a>human: `"empty"` |
| <a id="s-1d65c30d97"></a>`confirmation-declined` | <a id="s-0727671a92"></a>`{"kind":"interactive-confirmation-mismatch"}` | <a id="s-9c709b4eea"></a>`1` | <a id="s-29e97bb1de"></a>human: `"noncontractual-presentation-of-command-result"` | <a id="s-bb1b7af094"></a>human: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-2d7e5e454c) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --confirm](#s-64685d54c7) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --dry-run](#s-164339ed67) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --json](#s-b8eaec0e8f) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/discard-plan](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-discard-plan.md)
- [POST /v1/collection-upload-sessions/{collection_id}/discard](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-discard.md)
- [riverhog_client.ApiClient.discard_collection_upload](../../riverhog-client/python/riverhog-client-apiclient-discard-collection-upload.md)
- [riverhog_client.ApiClient.plan_collection_upload_discard](../../riverhog-client/python/riverhog-client-apiclient-plan-collection-upload-discard.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-6363da1d68"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-fbf4bbaa8d"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::upload\_discard\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2463)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/discard/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/discard/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/discard/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/discard/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/discard/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/discard/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/discard/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/discard/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/discard/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/discard/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/discard/name`

<!-- exact-contract-value: 27e5d8fcb7e7c0c194453fff8dcce54dbd2cec00c0d18c995023ce62b981b7da -->

```json
"discard"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/discard/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/discard/result_contract`

<!-- exact-contract-value: bffebba5da36df90c87e1f310923e6da4f8932b8b3db14a9d1029cd673a264f4 -->

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
  "identity": "a-riverhog-cli-result/collection/upload/discard/v1",
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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/discard/terminating_controls`

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
