# piggity collection provenance show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance-show:82d3b8f626 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-e3cd0a43db"></a>Parser name: `show`
- <a id="s-3c111bdb6a"></a>Extra arguments at this parser: rejected.
- <a id="s-e6eeda41e7"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-eaa32bf593"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-264a1748ef"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-157533c3ca"></a>`path`<br>`path` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-118774fada"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-43b5eb78d9"></a>`help` | <a id="s-d3789afb22"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-08b599f85d"></a>`0` | <a id="s-6718447576"></a>`"noncontractual-framework-help"` | <a id="s-f07429935e"></a>`"empty"` |

### Result and failure contract

- <a id="s-b0c1e318a9"></a>Result identity: `piggity-cli-result/collection/provenance/show/v1`
- <a id="s-377b08a61c"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-40fae5b7d3"></a>Structured output: `optional-json`
- <a id="s-a025ffcaeb"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6f8c823624"></a>`completed` | <a id="s-ecc7ffcdcb"></a>`{"kind":"command-completed"}` | <a id="s-4df9534047"></a>`0` | <a id="s-ac7d9d9a4c"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_collection_file_provenance response 200](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-files-path.md#s-15bacadd6f) | <a id="s-e7a7c9c72b"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-84612799e9"></a>`usage` | <a id="s-030d76e2aa"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-7456b95d99"></a>`2` | <a id="s-14d81ff448"></a>all: `"empty"` | <a id="s-8b2b4ffb6e"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-b7af29cf0b"></a>`operational` | <a id="s-a662dcfcca"></a>`{"kind":"application-error"}` | <a id="s-c4935c6b8f"></a>`1` | <a id="s-eceb9ea4b7"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-bd2d9f5ed2"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-264a1748ef) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-118774fada) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter path](#s-157533c3ca) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/files/{path}](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-files-path.md)
- [riverhog_client.ApiClient.get_collection_file_provenance](../../riverhog-client/python/riverhog-client-apiclient-get-collection-file-provenance.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-742fdd1081"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-0a778a9955"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources/authorities.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::provenance\_show\_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2623)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/parameters`

<!-- exact-contract-value: 5634b44414826da829550303b98e1286ca305c8d87b14ee3287aef4841c3c64d -->

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
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "path",
    "nargs": 1,
    "options": [
      "path"
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

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/result_contract`

<!-- exact-contract-value: 6b659c9ee4786a05538198b16cb60cd38f762607be44e7983ae5f029aa68a068 -->

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
  "identity": "piggity-cli-result/collection/provenance/show/v1",
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
          "operation_id": "get_collection_file_provenance",
          "path": "/v1/collections/{collection_id}/provenance/files/{path}",
          "schema": {
            "$ref": "#/components/schemas/CollectionFileProvenanceDetailOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/show/terminating_controls`

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
