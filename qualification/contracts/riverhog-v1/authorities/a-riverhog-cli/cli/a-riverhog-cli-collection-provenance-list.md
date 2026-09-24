# a-riverhog-cli collection provenance list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-provenance-list:d19eac1272 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1661ab73c7"></a>Parser name: `list`
- <a id="s-b2b384f20c"></a>Extra arguments at this parser: rejected.
- <a id="s-d7491aed19"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-12d004a27f"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-b08ae84ce6"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-6ad4ba7d0e"></a>`page_size`<br>`--page-size` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `25`<br>Env: `null` |
| <a id="s-a40e03015a"></a>`page_token`<br>`--page-token` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-170d235b6d"></a>`sort`<br>`--sort` | optional option; 1 value | text | `"path"`<br>Env: `null` |
| <a id="s-eb7c5bd2c3"></a>`order`<br>`--order` | optional option; 1 value | text | `"asc"`<br>Env: `null` |
| <a id="s-6764d938ce"></a>`query`<br>`--query`, `-q` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-80bdac7af6"></a>`status`<br>`--status` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-4cdb3e8e70"></a>`selectors`<br>`--selectors` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-5e52ca836b"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-43ff3988f9"></a>`help` | <a id="s-e020555439"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-12e50f38cd"></a>`0` | <a id="s-43f616348c"></a>`"noncontractual-framework-help"` | <a id="s-03f97308e3"></a>`"empty"` |

### Result and failure contract

- <a id="s-5fbc946317"></a>Result identity: `a-riverhog-cli-result/collection/provenance/list/v1`
- <a id="s-68b332319c"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-a3bbb506ab"></a>Structured output: `optional-json`
- <a id="s-bead259a79"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-3674ed265e"></a>`completed` | <a id="s-c4d2fefea5"></a>`{"kind":"command-completed"}` | <a id="s-662399c3f6"></a>`0` | <a id="s-b215c40c4e"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP list_collection_provenance response 200](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-files.md#s-2344bff45a) | <a id="s-406a410731"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-9ff2d2471a"></a>`usage` | <a id="s-12a1209d2b"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-de48af96a0"></a>`2` | <a id="s-06d89c85f2"></a>all: `"empty"` | <a id="s-15eec2d1d0"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-7ce3cb2bc4"></a>`operational` | <a id="s-a06e7d44c7"></a>`{"kind":"application-error"}` | <a id="s-0c923b88ff"></a>`1` | <a id="s-0063d1ac6e"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-0184186fe1"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-b08ae84ce6) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-5e52ca836b) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --order](#s-eb7c5bd2c3) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-size](#s-6ad4ba7d0e) | `value · cli-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-6ad4ba7d0e) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-a40e03015a) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --query](#s-6764d938ce) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --selectors](#s-4cdb3e8e70) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --sort](#s-170d235b6d) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --status](#s-80bdac7af6) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/files](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-files.md)
- [riverhog_client.ApiClient.list_collection_provenance](../../riverhog-client/python/riverhog-client-apiclient-list-collection-provenance.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-a47fcfefaa"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-7b57311a14"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::provenance\_list\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2618)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/list/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/list/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/list/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/list/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/list/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/list/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/list/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/list/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/list/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/list/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/list/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/list/result_contract`

<!-- exact-contract-value: 9b501ab73a4a04c0043ac279858940c52b7e7833d46ddec2adc32fe4653687e1 -->

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
  "identity": "a-riverhog-cli-result/collection/provenance/list/v1",
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
          "operation_id": "list_collection_provenance",
          "path": "/v1/collections/{collection_id}/provenance/files",
          "schema": {
            "$ref": "#/components/schemas/ListCollectionFileProvenanceOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/list/terminating_controls`

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
