# a-riverhog-cli collection show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-show:0d49a410c7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-47d3b0545f"></a>Parser name: `show`
- <a id="s-816fe539a0"></a>Extra arguments at this parser: rejected.
- <a id="s-18bbba1953"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-754cb95235"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-ebc47d6f02"></a>`collection`<br>`collection` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-376d2760dc"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-a0db45e37e"></a>`help` | <a id="s-af83b7a3cf"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-fd274d2f59"></a>`0` | <a id="s-0c7945ea7b"></a>`"noncontractual-framework-help"` | <a id="s-ab96963a5d"></a>`"empty"` |

### Result and failure contract

- <a id="s-f7d3b26cd0"></a>Result identity: `a-riverhog-cli-result/collection/show/v1`
- <a id="s-83b82a8f35"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-a0c0c6be1d"></a>Structured output: `optional-json`
- <a id="s-8dee6bc533"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-bcbe9bbe13"></a>`completed` | <a id="s-16fa7a39c5"></a>`{"kind":"command-completed"}` | <a id="s-b3690ac377"></a>`0` | <a id="s-b8a1873130"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_collection response 200](../../riverhog/http-operations/get-v1-collections-collection-id.md#s-0efbe086d7) | <a id="s-d70f880cc8"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-dd0f0e4e26"></a>`usage` | <a id="s-0401af0ae9"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-9d616973e4"></a>`2` | <a id="s-edd4beb469"></a>all: `"empty"` | <a id="s-89c166b680"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-49c073b787"></a>`operational` | <a id="s-2a19bb5378"></a>`{"kind":"application-error"}` | <a id="s-85ba268ccb"></a>`1` | <a id="s-24726bd6fc"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-398ead81b8"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection](#s-ebc47d6f02) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-376d2760dc) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [riverhog_client.ApiClient.get_collection](../../riverhog-client/python/riverhog-client-apiclient-get-collection.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3aa63c5d3d"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-1ba14fede6"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::show\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2540)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/show/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/show/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/show/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/show/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/show/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/show/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/show/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/show/result_contract`

<!-- exact-contract-value: 5aa6d17bbef414ca3e8e22b9e1867efedd5d1498bfce0587865bbf0594fe6006 -->

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
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "a-riverhog-cli-result/collection/show/v1",
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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/show/terminating_controls`

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
