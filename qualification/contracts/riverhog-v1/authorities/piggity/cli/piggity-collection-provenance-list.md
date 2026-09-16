# piggity collection provenance list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance-list:4dd2f2b7e4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-7e26dc5607"></a>Parser name: `list`
- <a id="s-e661256935"></a>Extra arguments at this parser: rejected.
- <a id="s-a14ec77d7f"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-b667ee36f2"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-f5559afeee"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-01c3138346"></a>`page_size`<br>`--page-size` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `25`<br>Env: `null` |
| <a id="s-555db10fef"></a>`page_token`<br>`--page-token` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-67a62d1793"></a>`sort`<br>`--sort` | optional option; 1 value | text | `"path"`<br>Env: `null` |
| <a id="s-abb18d9ee3"></a>`order`<br>`--order` | optional option; 1 value | text | `"asc"`<br>Env: `null` |
| <a id="s-f5c1819bc3"></a>`query`<br>`--query`, `-q` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-c78d4198ac"></a>`status`<br>`--status` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-0a0adb418e"></a>`selectors`<br>`--selectors` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-22b246f5d2"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-65fd8acaa5"></a>`help` | <a id="s-516319e00c"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c262020bf1"></a>`0` | <a id="s-242f1f64fb"></a>`"noncontractual-framework-help"` | <a id="s-f12233495f"></a>`"empty"` |

### Result and failure contract

- <a id="s-a501468878"></a>Result identity: `piggity-cli-result/collection/provenance/list/v1`
- <a id="s-1f0543e41f"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-96c06907e6"></a>Structured output: `optional-json`
- <a id="s-4868206250"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c69ba19458"></a>`completed` | <a id="s-f87f47ba34"></a>`{"kind":"command-completed"}` | <a id="s-3ce2d746f0"></a>`0` | <a id="s-ba66612457"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP list_collection_provenance response 200](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-files.md#s-2344bff45a) | <a id="s-df96f4943d"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-48892309c9"></a>`usage` | <a id="s-ea9684f720"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-cbeb2bc869"></a>`2` | <a id="s-fbbe599760"></a>all: `"empty"` | <a id="s-3451e52d4e"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-c4b5221b74"></a>`operational` | <a id="s-b52aee9f6c"></a>`{"kind":"application-error"}` | <a id="s-33833c23b7"></a>`1` | <a id="s-997817a140"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-da11d27d29"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-f5559afeee) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-22b246f5d2) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --order](#s-abb18d9ee3) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-size](#s-01c3138346) | `value · cli-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-01c3138346) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-555db10fef) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --query](#s-f5c1819bc3) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --selectors](#s-0a0adb418e) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --sort](#s-67a62d1793) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --status](#s-c78d4198ac) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/files](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-files.md)
- [riverhog_client.ApiClient.list_collection_provenance](../../riverhog-client/python/riverhog-client-apiclient-list-collection-provenance.md)

## Governing policies

- <a id="pa-0023cfd324"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-510589c42e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::provenance_list_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2579)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/list/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/list/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/list/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/list/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/list/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/list/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/list/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/list/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/list/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/list/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/list/parameters`

<!-- exact-contract-value: 4110c3ba988be626a1827d33e90f80936c5d25e2018ee43578b9439e9705fcaf -->

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
    "default": 25,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "page_size",
    "nargs": 1,
    "options": [
      "--page-size"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "clamp": false,
      "class": "typer._click.types.IntRange",
      "max_open": false,
      "maximum": 100,
      "min_open": false,
      "minimum": 1,
      "name": "integer range"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "page_token",
    "nargs": 1,
    "options": [
      "--page-token"
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
    "default": "path",
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "sort",
    "nargs": 1,
    "options": [
      "--sort"
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
    "default": "asc",
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "order",
    "nargs": 1,
    "options": [
      "--order"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "query",
    "nargs": 1,
    "options": [
      "--query",
      "-q"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "status",
    "nargs": 1,
    "options": [
      "--status"
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
    "name": "selectors",
    "nargs": 1,
    "options": [
      "--selectors"
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

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/list/result_contract`

<!-- exact-contract-value: 15a9daaf0a66acaaef42d384c8de8349c4ee196db3b0eded4023c5ac54201c7f -->

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
  "identity": "piggity-cli-result/collection/provenance/list/v1",
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
          "operation_id": "list_collection_provenance",
          "path": "/v1/collections/{collection_id}/provenance/files",
          "schema": {
            "$ref": "#/components/schemas/ListCollectionFileProvenanceResponse"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/list/terminating_controls`

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
