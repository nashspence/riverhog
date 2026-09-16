# piggity local remove

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-remove:48d45fd242 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d4846c3908"></a>Parser name: `remove`
- <a id="s-485de536c7"></a>Extra arguments at this parser: rejected.
- <a id="s-2f63c9f5da"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-9f3a3edab6"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-0b8d9cb7e8"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded |
| <a id="s-64e6ad6b9d"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-130769ea69"></a>`help` | <a id="s-5e3b9e4ca2"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-b64f47aaa5"></a>`0` | <a id="s-fdd3ff246f"></a>`"noncontractual-framework-help"` | <a id="s-e61bd00ec1"></a>`"empty"` |

### Result and failure contract

- <a id="s-ee987dd032"></a>Result identity: `piggity-cli-result/local/remove/v1`
- <a id="s-edaef51bab"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-abc784e04f"></a>Structured output: `optional-json`
- <a id="s-313608f4bc"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-d4b6e13a20"></a>`completed` | <a id="s-a4a80982e7"></a>`{"kind":"command-completed"}` | <a id="s-36c96dfa29"></a>`0` | <a id="s-dcf6b4bf36"></a>human: `noncontractual-presentation-of-command-result`; json: [piggity-local-remove-result/v1](#s-12188a0fdb) | <a id="s-5c9954c18a"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f0f5cd3d2a"></a>`usage` | <a id="s-1414cc91fc"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-ea2cc73865"></a>`2` | <a id="s-1fb6f1fee2"></a>all: `empty` | <a id="s-1db40d339e"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-822795332e"></a>`operational` | <a id="s-cb0abd90f6"></a>`{"kind":"application-error"}` | <a id="s-5b45e87b22"></a>`1` | <a id="s-8236970d8a"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-b213eea07f"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Local structured outputs


#### <a id="s-12188a0fdb"></a>`piggity-local-remove-result/v1`

Applies to: completed · stdout (json).

<a id="s-60df9ab97f"></a>

- <a id="s-7edbfc77ab"></a>`type`: `"object"`
- <a id="s-51b3b6b731"></a>`additionalProperties`: `false`
- <a id="s-06bf59c225"></a>`required`: `["status","collection_id","local_files","retrievals_canceled"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1464eecd13"></a>`collection_id` | yes | type="integer"; minimum=1 |  |
| <a id="s-c8a7e28142"></a>`local_files` | yes | const="retained" |  |
| <a id="s-6e5dd743b1"></a>`retrievals_canceled` | yes | type="array"; items=(type="string") |  |
| <a id="s-c44734abdd"></a>`status` | yes | const="removed" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-0b8d9cb7e8) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-64e6ad6b9d) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [DELETE /v1/retrieval-jobs/{job_id}](../../riverhog/http-operations/delete-v1-retrieval-jobs-job-id.md)
- [GET /v1/retrieval-jobs/{job_id}](../../riverhog/http-operations/get-v1-retrieval-jobs-job-id.md)
- [riverhog_client.ApiClient.cancel_retrieval_job](../../riverhog-client/python/riverhog-client-apiclient-cancel-retrieval-job.md)
- [riverhog_client.ApiClient.get_retrieval_job](../../riverhog-client/python/riverhog-client-apiclient-get-retrieval-job.md)

## Governing policies

- <a id="pa-cca1fe01f1"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-699e05ffa6"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/local.py::remove_collection](../../../../../../reference/riverhog/applications/piggity/src/piggity/local.py#L917)

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/remove/allow_extra_args`
- `/external_contract/cli/piggity/commands/local/commands/remove/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/local/commands/remove/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/local/commands/remove/name`
- `/external_contract/cli/piggity/commands/local/commands/remove/parameters`
- `/external_contract/cli/piggity/commands/local/commands/remove/result_contract`
- `/external_contract/cli/piggity/commands/local/commands/remove/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/remove/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/remove/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/local/commands/remove/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/remove/name`

<!-- exact-contract-value: 66f3d68b7627c9f6965029d01923daefa869b3f33123c145ddb25464b41a529a -->

```json
"remove"
```

### `/external_contract/cli/piggity/commands/local/commands/remove/parameters`

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

### `/external_contract/cli/piggity/commands/local/commands/remove/result_contract`

<!-- exact-contract-value: 94957569b02f9c417067bfd90289cfc4dc92de9fe0da26c2b934653416e49bc9 -->

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
  "identity": "piggity-cli-result/local/remove/v1",
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
          "identity": "piggity-local-remove-result/v1",
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

### `/external_contract/cli/piggity/commands/local/commands/remove/terminating_controls`

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
