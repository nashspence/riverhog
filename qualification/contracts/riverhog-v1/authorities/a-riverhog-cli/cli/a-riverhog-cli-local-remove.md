# a-riverhog-cli local remove

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-local-remove:512cb725fc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-8b31306851"></a>Parser name: `remove`
- <a id="s-fdbcaa3bcc"></a>Extra arguments at this parser: rejected.
- <a id="s-e159941c3d"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-72c61cb3ba"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-7bee974d8e"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-392aa2dc9b"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c5fe6aaf19"></a>`help` | <a id="s-4515f0b0f8"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-59c5d41a77"></a>`0` | <a id="s-06dbc41560"></a>`"noncontractual-framework-help"` | <a id="s-8adaea9609"></a>`"empty"` |

### Result and failure contract

- <a id="s-8de03e2836"></a>Result identity: `a-riverhog-cli-result/local/remove/v1`
- <a id="s-f14c689a28"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-b04e149209"></a>Structured output: `optional-json`
- <a id="s-0659ee6ad8"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-9e2eb5bcc9"></a>`completed` | <a id="s-419b774785"></a>`{"kind":"command-completed"}` | <a id="s-ba148fc112"></a>`0` | <a id="s-093dfd1348"></a>human: `"noncontractual-presentation-of-command-result"`; json: [a-riverhog-cli-local-remove-result/v1](#s-5b9a13b397) | <a id="s-c36d855828"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-fe81709214"></a>`usage` | <a id="s-2615bb3a81"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-7ed65cb1aa"></a>`2` | <a id="s-37cce62239"></a>all: `"empty"` | <a id="s-90a3a817fa"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-cf9abf4cce"></a>`operational` | <a id="s-af8bb46227"></a>`{"kind":"application-error"}` | <a id="s-6acb1e080d"></a>`1` | <a id="s-eeb1ea9ddb"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-a0702652cd"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-5b9a13b397"></a>`a-riverhog-cli-local-remove-result/v1`

Applies to: completed · stdout (json).

<a id="s-594ac02508"></a>

- <a id="s-0a40e44a7b"></a>`type`: `"object"`
- <a id="s-92922abb59"></a>`additionalProperties`: `false`
- <a id="s-0cffa4c1d0"></a>`required`: `["status","collection_id","local_files","retrievals_canceled"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b8081f1681"></a>`collection_id` | yes | type="integer"; minimum=1 |  |
| <a id="s-eb5dde2dec"></a>`local_files` | yes | const="retained" |  |
| <a id="s-e63fd374ca"></a>`retrievals_canceled` | yes | type="array"; items=(type="string") |  |
| <a id="s-26ea8ec71d"></a>`status` | yes | const="removed" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-7bee974d8e) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-392aa2dc9b) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [DELETE /v1/retrieval-jobs/{job_id}](../../riverhog/http-operations/delete-v1-retrieval-jobs-job-id.md)
- [GET /v1/retrieval-jobs/{job_id}](../../riverhog/http-operations/get-v1-retrieval-jobs-job-id.md)
- [riverhog_client.ApiClient.cancel_retrieval_job](../../riverhog-client/python/riverhog-client-apiclient-cancel-retrieval-job.md)
- [riverhog_client.ApiClient.get_retrieval_job](../../riverhog-client/python/riverhog-client-apiclient-get-retrieval-job.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-4af925a919"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-ccba8d47fb"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/local.py::remove\_collection](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/local.py#L917)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/local/commands/remove/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/remove/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/remove/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/remove/name`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/remove/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/remove/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/remove/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/remove/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/remove/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/remove/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/remove/name`

<!-- exact-contract-value: 66f3d68b7627c9f6965029d01923daefa869b3f33123c145ddb25464b41a529a -->

```json
"remove"
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/remove/parameters`

<!-- exact-contract-value: 69121b7dd4df39852c314f302ca34fb358e3d4472f9e5565bdec50242e30ee3a -->

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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/remove/result_contract`

<!-- exact-contract-value: 25dbbb8e60d7040e22121ae6d6eef2eeb71344f15fb64a6dfa3069baba4fe897 -->

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
  "identity": "a-riverhog-cli-result/local/remove/v1",
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
          "identity": "a-riverhog-cli-local-remove-result/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "collection_id": {
                "minimum": 1,
                "type": "integer"
              },
              "local_files": {
                "const": "retained"
              },
              "retrievals_canceled": {
                "items": {
                  "type": "string"
                },
                "type": "array"
              },
              "status": {
                "const": "removed"
              }
            },
            "required": [
              "status",
              "collection_id",
              "local_files",
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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/remove/terminating_controls`

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
