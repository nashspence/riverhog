# a-riverhog-cli collection describe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-describe:2b89800e17 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-24521da664"></a>Parser name: `describe`
- <a id="s-f7bf461fb6"></a>Extra arguments at this parser: rejected.
- <a id="s-889376a889"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-76023a6c8b"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-6f0bbdc60e"></a>`collection`<br>`collection` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-355c727d47"></a>`description`<br>`--description` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-b09726e824"></a>`clear`<br>`--clear` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-c2996dd229"></a>`if_match`<br>`--if-match` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-d3581ce60b"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-7a13ef1334"></a>`help` | <a id="s-d87467693c"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c6be2723a7"></a>`0` | <a id="s-7748e2fc80"></a>`"noncontractual-framework-help"` | <a id="s-61c4044f86"></a>`"empty"` |

### Result and failure contract

- <a id="s-d27253616f"></a>Result identity: `a-riverhog-cli-result/collection/describe/v1`
- <a id="s-160c879953"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-c33d1e11f7"></a>Structured output: `optional-json`
- <a id="s-3715e7e334"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-30b2a9edfe"></a>`completed` | <a id="s-3b4e0c1a10"></a>`{"kind":"command-completed"}` | <a id="s-2f71ef9b7d"></a>`0` | <a id="s-bf58d69a44"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP replace_collection_description response 200](../../riverhog/http-operations/put-v1-collections-collection-id-description.md#s-2c0b359078) | <a id="s-646f5ec718"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5365b4789f"></a>`usage` | <a id="s-f5e72b9f06"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-14113c49ee"></a>`2` | <a id="s-0efa87cae1"></a>all: `"empty"` | <a id="s-7f19c31f20"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-1ff5543327"></a>`operational` | <a id="s-6e6c36284c"></a>`{"kind":"application-error"}` | <a id="s-fa5c0c5657"></a>`1` | <a id="s-9244deaf95"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-ca4c76a501"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --clear](#s-b09726e824) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter collection](#s-6f0bbdc60e) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --description](#s-355c727d47) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --if-match](#s-c2996dd229) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-d3581ce60b) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [PUT /v1/collections/{collection_id}/description](../../riverhog/http-operations/put-v1-collections-collection-id-description.md)
- [riverhog_client.ApiClient.get_collection](../../riverhog-client/python/riverhog-client-apiclient-get-collection.md)
- [riverhog_client.ApiClient.replace_collection_description](../../riverhog-client/python/riverhog-client-apiclient-replace-collection-description.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b7045d6a86"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-5fc795aa55"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::collection\_describe\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2551)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/describe/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/describe/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/describe/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/describe/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/describe/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/describe/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/describe/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/describe/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/describe/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/describe/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/describe/name`

<!-- exact-contract-value: ccbb85a554fc61cc780e2cae6cc0e75e15a01539011884b8e460657a860ded8e -->

```json
"describe"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/describe/parameters`

<!-- exact-contract-value: 645f658239e15189970ff762b7508b23e08fa999995a7f8ecbed88e0376a7925 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "collection",
    "nargs": 1,
    "options": [
      "collection"
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
    "name": "description",
    "nargs": 1,
    "options": [
      "--description"
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
    "name": "clear",
    "nargs": 1,
    "options": [
      "--clear"
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
    "name": "if_match",
    "nargs": 1,
    "options": [
      "--if-match"
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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/describe/result_contract`

<!-- exact-contract-value: 0175b68519d0c54c5c8e13b3e923a04d86ad88ae2d144f19ab7533c23d789cea -->

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
  "identity": "a-riverhog-cli-result/collection/describe/v1",
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
          "method": "PUT",
          "operation_id": "replace_collection_description",
          "path": "/v1/collections/{collection_id}/description",
          "schema": {
            "$ref": "#/components/schemas/CollectionDescriptionOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/describe/terminating_controls`

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
