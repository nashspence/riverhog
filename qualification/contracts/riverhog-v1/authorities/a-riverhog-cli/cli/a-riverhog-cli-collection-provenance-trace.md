# a-riverhog-cli collection provenance trace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-provenance-trace:3a5f720a26 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-f33ecb1e88"></a>Parser name: `trace`
- <a id="s-15a8497064"></a>Extra arguments at this parser: rejected.
- <a id="s-9a4f8f7999"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-caf4f1a753"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-bab07cbd93"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-4906009f58"></a>`path`<br>`path` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-25215d732a"></a>`page_size`<br>`--page-size` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `25`<br>Env: `null` |
| <a id="s-59a95c74ca"></a>`page_token`<br>`--page-token` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-76d30989ad"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-022f69f328"></a>`help` | <a id="s-b318288887"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-669155c97b"></a>`0` | <a id="s-56a28e1cd6"></a>`"noncontractual-framework-help"` | <a id="s-19fb209795"></a>`"empty"` |

### Result and failure contract

- <a id="s-4482368c23"></a>Result identity: `a-riverhog-cli-result/collection/provenance/trace/v1`
- <a id="s-2ee7190fc2"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-96f0b24932"></a>Structured output: `optional-json`
- <a id="s-6127c1e716"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-862e77432c"></a>`completed` | <a id="s-894dfc3291"></a>`{"kind":"command-completed"}` | <a id="s-7c175f528a"></a>`0` | <a id="s-cdf5e1ed3f"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP trace_collection_file_provenance response 200](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-trace-path.md#s-88d1d82fa6) | <a id="s-0d68733ba4"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-24b5a3a7aa"></a>`usage` | <a id="s-796373a4ee"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-2e4082e7a4"></a>`2` | <a id="s-397a8e1971"></a>all: `"empty"` | <a id="s-2955f1fb1a"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-b56e4052b4"></a>`operational` | <a id="s-16829d4099"></a>`{"kind":"application-error"}` | <a id="s-61a0698f75"></a>`1` | <a id="s-e4e972f905"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-71bfa62101"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-bab07cbd93) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-76d30989ad) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --page-size](#s-25215d732a) | `value · cli-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-25215d732a) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-59a95c74ca) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter path](#s-4906009f58) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/trace/{path}](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-trace-path.md)
- [riverhog_client.ApiClient.trace_collection_file_provenance](../../riverhog-client/python/riverhog-client-apiclient-trace-collection-file-provenance.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-978b253b19"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-7ebd11a581"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::provenance\_trace\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2674)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/trace/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/trace/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/trace/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/trace/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/trace/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/trace/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/trace/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/trace/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/trace/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/trace/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/trace/name`

<!-- exact-contract-value: c9613997237f65491e06a014bcfff2df59b60534cf4c5d1eaf001e4764d4f15b -->

```json
"trace"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/trace/parameters`

<!-- exact-contract-value: f80514bca7709ad11bbecc3a709c60d3c98d3ead69e18703748ee262f5d3f695 -->

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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/trace/result_contract`

<!-- exact-contract-value: 464e2352abad013ecbd71c352ec7de37a5f9400be6180d9d5ebe64b2b7143834 -->

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
  "identity": "a-riverhog-cli-result/collection/provenance/trace/v1",
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
          "operation_id": "trace_collection_file_provenance",
          "path": "/v1/collections/{collection_id}/provenance/trace/{path}",
          "schema": {
            "$ref": "#/components/schemas/CollectionFileProvenanceTraceOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/trace/terminating_controls`

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
