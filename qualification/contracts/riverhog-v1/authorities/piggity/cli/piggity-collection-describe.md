# piggity collection describe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-describe:2e88432e6b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-2dd51f2dda"></a>Parser name: `describe`
- <a id="s-568bd79408"></a>Extra arguments at this parser: rejected.
- <a id="s-e34e169c9e"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-d7cfa461a6"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-a79c8f4d1c"></a>`collection`<br>`collection` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-c6fc67bf05"></a>`description`<br>`--description` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-6146b8bc01"></a>`clear`<br>`--clear` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-fe5337d00f"></a>`if_match`<br>`--if-match` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-92d36de323"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-b1a7bfa56f"></a>`help` | <a id="s-57da332599"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-e5a1a90e8a"></a>`0` | <a id="s-f6e2f4606f"></a>`"noncontractual-framework-help"` | <a id="s-c3bc7827bd"></a>`"empty"` |

### Result and failure contract

- <a id="s-73507093b9"></a>Result identity: `piggity-cli-result/collection/describe/v1`
- <a id="s-0017d54374"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-6db063e197"></a>Structured output: `optional-json`
- <a id="s-ae630e36a3"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-8a9d35f61d"></a>`completed` | <a id="s-1513ec233d"></a>`{"kind":"command-completed"}` | <a id="s-8b7a9a3b5a"></a>`0` | <a id="s-efb4b82fe7"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP replace_collection_description response 200](../../riverhog/http-operations/put-v1-collections-collection-id-description.md#s-2c0b359078) | <a id="s-26df4d777f"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-9633bce9bc"></a>`usage` | <a id="s-03dccb19ab"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-da0edf292e"></a>`2` | <a id="s-8a528d9508"></a>all: `"empty"` | <a id="s-a1102f7496"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-802745aa94"></a>`operational` | <a id="s-13de4c4448"></a>`{"kind":"application-error"}` | <a id="s-aa10a762c2"></a>`1` | <a id="s-07a9c73ae3"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-2ad6252e1a"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --clear](#s-6146b8bc01) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter collection](#s-a79c8f4d1c) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --description](#s-c6fc67bf05) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --if-match](#s-fe5337d00f) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-92d36de323) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [PUT /v1/collections/{collection_id}/description](../../riverhog/http-operations/put-v1-collections-collection-id-description.md)
- [riverhog_client.ApiClient.get_collection](../../riverhog-client/python/riverhog-client-apiclient-get-collection.md)
- [riverhog_client.ApiClient.replace_collection_description](../../riverhog-client/python/riverhog-client-apiclient-replace-collection-description.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-0dc8f5a626"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-00c1941e34"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources/authorities.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::collection\_describe\_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2536)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/describe/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/describe/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/describe/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/describe/name`
- `/external_contract/cli/piggity/commands/collection/commands/describe/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/describe/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/describe/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/describe/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/describe/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/describe/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/describe/name`

<!-- exact-contract-value: ccbb85a554fc61cc780e2cae6cc0e75e15a01539011884b8e460657a860ded8e -->

```json
"describe"
```

### `/external_contract/cli/piggity/commands/collection/commands/describe/parameters`

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

### `/external_contract/cli/piggity/commands/collection/commands/describe/result_contract`

<!-- exact-contract-value: aee6e6212028db85a1538ca9db8955260b28dd21e31ef6d5696cbbf26a0025b9 -->

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
  "identity": "piggity-cli-result/collection/describe/v1",
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

### `/external_contract/cli/piggity/commands/collection/commands/describe/terminating_controls`

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
