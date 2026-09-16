# piggity find

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-find:6ee82d5c15 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-b080b673f2"></a>Parser name: `find`
- <a id="s-4b72f0f253"></a>Extra arguments at this parser: rejected.
- <a id="s-df0cd025c2"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-3a7e71ee8b"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-8fa2213b0a"></a>`query`<br>`--query`, `-q` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-ebc66f0d6c"></a>`page_size`<br>`--page-size` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `25`<br>Env: `null` |
| <a id="s-d65074f125"></a>`page_token`<br>`--page-token` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-d85824fb9f"></a>`sort`<br>`--sort` | optional option; 1 value | text | `"file_ref"`<br>Env: `null` |
| <a id="s-4813cf8a14"></a>`order`<br>`--order` | optional option; 1 value | text | `"asc"`<br>Env: `null` |
| <a id="s-5956372d81"></a>`collection`<br>`--collection` | optional option; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-069125d6c8"></a>`selectors`<br>`--selectors` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-e47552dc3e"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-28c4d85096"></a>`help` | <a id="s-e133c6dc13"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-888bca4f9d"></a>`0` | <a id="s-b6371ad5fa"></a>`"noncontractual-framework-help"` | <a id="s-af963e9562"></a>`"empty"` |

### Result and failure contract

- <a id="s-e42d1c6e60"></a>Result identity: `piggity-cli-result/find/v1`
- <a id="s-37b36d95d7"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-f17185a092"></a>Structured output: `optional-json`
- <a id="s-f8d41484d6"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-d1a5d4a0f7"></a>`completed` | <a id="s-bf4ec993b3"></a>`{"kind":"command-completed"}` | <a id="s-21814263e0"></a>`0` | <a id="s-4197873b59"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP search response 200](../../riverhog/http-operations/get-v1-search.md#s-776e2cba8a) | <a id="s-47743f31be"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-38485143ab"></a>`usage` | <a id="s-98d7b11aca"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-2cbf779545"></a>`2` | <a id="s-9df05f2847"></a>all: `"empty"` | <a id="s-5830bc0730"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-45a520526c"></a>`operational` | <a id="s-d0f9e7734b"></a>`{"kind":"application-error"}` | <a id="s-44cdaf3dfe"></a>`1` | <a id="s-43ea873930"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-cc05b3a358"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --collection](#s-5956372d81) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-e47552dc3e) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --order](#s-4813cf8a14) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-size](#s-ebc66f0d6c) | `value · cli-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-ebc66f0d6c) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-d65074f125) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --query](#s-8fa2213b0a) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --selectors](#s-069125d6c8) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --sort](#s-d85824fb9f) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/search](../../riverhog/http-operations/get-v1-search.md)
- [riverhog_client.ApiClient.search](../../riverhog-client/python/riverhog-client-apiclient-search.md)

## Governing policies

- <a id="pa-aa7de01a40"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-9ce4125f1c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::find_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2478)

### Machine authority

- `/external_contract/cli/piggity/commands/find/allow_extra_args`
- `/external_contract/cli/piggity/commands/find/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/find/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/find/name`
- `/external_contract/cli/piggity/commands/find/parameters`
- `/external_contract/cli/piggity/commands/find/result_contract`
- `/external_contract/cli/piggity/commands/find/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/find/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/find/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/find/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/find/name`

<!-- exact-contract-value: 3fbe8ae37de255630548b9d994d85052f2fcd10992224eb3f75f1ed28b1231f4 -->

```json
"find"
```

### `/external_contract/cli/piggity/commands/find/parameters`

<!-- exact-contract-value: f6b0d3ebe508980a6d19f3b5dda48b1d8e60333374eadeb0da2873b2ecf6f6b7 -->

```json
[
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
    "default": "file_ref",
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
    "name": "collection",
    "nargs": 1,
    "options": [
      "--collection"
    ],
    "required": false,
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

### `/external_contract/cli/piggity/commands/find/result_contract`

<!-- exact-contract-value: 41f49d647d1257e30ec68300ced5515d136202fdfe21d1e0361ae058f9c0bb47 -->

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
  "identity": "piggity-cli-result/find/v1",
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
          "operation_id": "search",
          "path": "/v1/search",
          "schema": {
            "$ref": "#/components/schemas/SearchResponse"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/find/terminating_controls`

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
