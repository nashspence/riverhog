# a-riverhog-cli local evict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-local-evict:04e85582ea -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-656e6b1295"></a>Parser name: `evict`
- <a id="s-e214b0a422"></a>Extra arguments at this parser: rejected.
- <a id="s-fba6c30b89"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-ad1ae8cc43"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-495bbc35b5"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-6cfcd297fd"></a>`confirm`<br>`--confirm`, alternate: `--no-confirm` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-8200cd305b"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-aae0c32d04"></a>`help` | <a id="s-9bf4fca4cf"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-64e98b17cd"></a>`0` | <a id="s-b9747e6d5c"></a>`"noncontractual-framework-help"` | <a id="s-2ed7c3ade2"></a>`"empty"` |

### Result and failure contract

- <a id="s-0b3c257577"></a>Result identity: `a-riverhog-cli-result/local/evict/v1`
- <a id="s-5aee5cf48d"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-825fbdf7f0"></a>Structured output: `optional-json`
- <a id="s-5e77069716"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-2d63112a5c"></a>`completed` | <a id="s-d05b61e21a"></a>`{"kind":"command-completed"}` | <a id="s-f7b40e0686"></a>`0` | <a id="s-5bef8fe1f9"></a>human: `"noncontractual-presentation-of-command-result"`; json: [a-riverhog-cli-local-evict-result/v1](#s-0a5d8a923e) | <a id="s-378bf27b0a"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-fb0a7cbc79"></a>`usage` | <a id="s-6f13ad6279"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-ebb4490466"></a>`2` | <a id="s-a15eb92f5e"></a>all: `"empty"` | <a id="s-41a775242b"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-19ddaadcf0"></a>`operational` | <a id="s-cfae76bdbf"></a>`{"kind":"application-error"}` | <a id="s-c24b0a841f"></a>`1` | <a id="s-2fe0bb026d"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-9f30ae83da"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-0a5d8a923e"></a>`a-riverhog-cli-local-evict-result/v1`

Applies to: completed · stdout (json).

<a id="s-28da2283d2"></a>

- <a id="s-24c1b4ccaa"></a>`type`: `"object"`
- <a id="s-ccdd17efb9"></a>`additionalProperties`: `false`
- <a id="s-8dceee1f7f"></a>`required`: `["status","collection_id","retrievals_canceled"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-94f9237b87"></a>`collection_id` | yes | type="integer"; minimum=1 |  |
| <a id="s-4b483f5165"></a>`retrievals_canceled` | yes | type="array"; items=(type="string") |  |
| <a id="s-fc9c7e760c"></a>`status` | yes | const="evicted" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-495bbc35b5) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --confirm](#s-6cfcd297fd) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --json](#s-8200cd305b) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [DELETE /v1/retrieval-jobs/{job_id}](../../riverhog/http-operations/delete-v1-retrieval-jobs-job-id.md)
- [GET /v1/retrieval-jobs/{job_id}](../../riverhog/http-operations/get-v1-retrieval-jobs-job-id.md)
- [riverhog_client.ApiClient.cancel_retrieval_job](../../riverhog-client/python/riverhog-client-apiclient-cancel-retrieval-job.md)
- [riverhog_client.ApiClient.get_retrieval_job](../../riverhog-client/python/riverhog-client-apiclient-get-retrieval-job.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-438a51a388"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-6f19210cb2"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/local.py::evict](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/local.py#L1189)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/local/commands/evict/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/evict/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/evict/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/evict/name`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/evict/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/evict/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/evict/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/evict/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/evict/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/evict/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/evict/name`

<!-- exact-contract-value: 90a4b686887bd1a4444a5b25700727402328babe6e5a215000cb5712dad96460 -->

```json
"evict"
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/evict/parameters`

<!-- exact-contract-value: 4efee97b1e0b5efebedc88669c125863abe67f9cc6fca582ec13c75c2c67664c -->

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
    "name": "confirm",
    "nargs": 1,
    "options": [
      "--confirm"
    ],
    "required": false,
    "secondary_options": [
      "--no-confirm"
    ],
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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/evict/result_contract`

<!-- exact-contract-value: e15d69aa73ed1acce961b9cea051448247a5fdcfc70b8443d432172686a61823 -->

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
  "identity": "a-riverhog-cli-result/local/evict/v1",
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
          "identity": "a-riverhog-cli-local-evict-result/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "collection_id": {
                "minimum": 1,
                "type": "integer"
              },
              "retrievals_canceled": {
                "items": {
                  "type": "string"
                },
                "type": "array"
              },
              "status": {
                "const": "evicted"
              }
            },
            "required": [
              "status",
              "collection_id",
              "retrievals_canceled"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/evict/terminating_controls`

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
