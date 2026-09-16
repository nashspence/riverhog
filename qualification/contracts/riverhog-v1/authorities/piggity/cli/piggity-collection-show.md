# piggity collection show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-show:cd2dae321c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-41cc6af641"></a>Parser name: `show`
- <a id="s-b91227aefd"></a>Extra arguments at this parser: rejected.
- <a id="s-c8a92055c2"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-4bd888302b"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-c9248428f9"></a>`collection`<br>`collection` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-29a6dc6d91"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-0f973f0758"></a>`help` | <a id="s-bd1d050d4d"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-f6aedebf16"></a>`0` | <a id="s-8087b9d6b8"></a>`"noncontractual-framework-help"` | <a id="s-b06a705a9e"></a>`"empty"` |

### Result and failure contract

- <a id="s-2d0a77594f"></a>Result identity: `piggity-cli-result/collection/show/v1`
- <a id="s-6fee1d7100"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-9bca7a59e5"></a>Structured output: `optional-json`
- <a id="s-8b8b3499ed"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b6d68cc861"></a>`completed` | <a id="s-a257cd1258"></a>`{"kind":"command-completed"}` | <a id="s-53c9e8dce5"></a>`0` | <a id="s-175677b000"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_collection response 200](../../riverhog/http-operations/get-v1-collections-collection-id.md#s-0efbe086d7) | <a id="s-dfc66cdeaf"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-aa3e75ffcc"></a>`usage` | <a id="s-4fabf2a9ec"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-d2b1bb3ca1"></a>`2` | <a id="s-87d771fe4b"></a>all: `"empty"` | <a id="s-670100a6e1"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-f3ee344376"></a>`operational` | <a id="s-0a2f0ca535"></a>`{"kind":"application-error"}` | <a id="s-d2d1571aed"></a>`1` | <a id="s-532c378f9f"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-4eba19ab72"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection](#s-c9248428f9) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-29a6dc6d91) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [riverhog_client.ApiClient.get_collection](../../riverhog-client/python/riverhog-client-apiclient-get-collection.md)

## Governing policies

- <a id="pa-04303a13f7"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-7651cf090b"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::show_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2525)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/show/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/show/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/show/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/show/name`
- `/external_contract/cli/piggity/commands/collection/commands/show/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/show/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/piggity/commands/collection/commands/show/parameters`

<!-- exact-contract-value: 511a78daf004d0bfabe4b353bb4365c33516bdeb388a361e7f74126afae5df81 -->

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

### `/external_contract/cli/piggity/commands/collection/commands/show/result_contract`

<!-- exact-contract-value: d1b7534b0d5956aa8786319fd77f3899c07080b273dc0655e6cc144414784aba -->

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
  "identity": "piggity-cli-result/collection/show/v1",
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
          "operation_id": "get_collection",
          "path": "/v1/collections/{collection_id}",
          "schema": {
            "$ref": "#/components/schemas/CollectionSummaryOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/show/terminating_controls`

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
