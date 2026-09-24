# a-riverhog-cli local repair

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-local-repair:105ba1eae9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-ffa4551dc4"></a>Parser name: `repair`
- <a id="s-3f9b684e89"></a>Extra arguments at this parser: rejected.
- <a id="s-0c21e296cb"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-44a0d59c78"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-eaa980d96c"></a>`wait`<br>`--wait`, alternate: `--no-wait` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-0296da92c8"></a>`restore_policy`<br>`--restore-policy` | optional option; 1 value | text | `"allow"`<br>Env: `null` |
| <a id="s-07b67e2896"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-bf925e2701"></a>`help` | <a id="s-ffc2474f27"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-4f094a8d24"></a>`0` | <a id="s-97bda7361c"></a>`"noncontractual-framework-help"` | <a id="s-a40821307d"></a>`"empty"` |

### Result and failure contract

- <a id="s-d08ebafa50"></a>Result identity: `a-riverhog-cli-result/local/repair/v1`
- <a id="s-7d4e79fe51"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-55dcff0174"></a>Structured output: `optional-json`
- <a id="s-5a3bdfcdfe"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-fc3309daeb"></a>`completed` | <a id="s-60198fc4e3"></a>`{"kind":"command-completed"}` | <a id="s-10e3e6a3d1"></a>`0` | <a id="s-e3f27d9d5d"></a>human: `"noncontractual-presentation-of-command-result"`; json: [a-riverhog-cli-local-sync-result/v1](#s-b01559055a) | <a id="s-3f80d982af"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-85cc6d6b11"></a>`usage` | <a id="s-0b2e6a676f"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-21badc4aa8"></a>`2` | <a id="s-9a6f5eafa3"></a>all: `"empty"` | <a id="s-38b29de20a"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-13ce91fbcd"></a>`operational` | <a id="s-21368bd6f7"></a>`{"kind":"application-error"}` | <a id="s-fae9090d6a"></a>`1` | <a id="s-ca863e07ac"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-33640f6984"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-b01559055a"></a>`a-riverhog-cli-local-sync-result/v1`

Applies to: completed · stdout (json).

<a id="s-61e0bfd302"></a>

- <a id="s-70760b509b"></a>`type`: `"object"`
- <a id="s-12448e0b67"></a>`additionalProperties`: `false`
- <a id="s-ea065f417e"></a>`required`: `["status","materialized_files"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e17326bb3c"></a>`materialized_files` | yes | type="integer"; minimum=0 |  |
| <a id="s-37bc3792be"></a>`restore_policy` | no | enum=["allow","never"] |  |
| <a id="s-9828f2f3b9"></a>`retrieval` | no | type="object" |  |
| <a id="s-882915aca0"></a>`retrieval_id` | no | type="string" |  |
| <a id="s-4ebd1dabe7"></a>`status` | yes | enum=["requested","ready","cache-miss","current","materialized"] |  |
| <a id="s-57beee08cb"></a>`unavailable_files` | no | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-07b67e2896) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --restore-policy](#s-0296da92c8) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --wait](#s-eaa980d96c) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [GET /v1/catalog/collections/{collection_id}/inventory](../../riverhog/http-operations/get-v1-catalog-collections-collection-id-inventory.md)
- [GET /v1/collections/{collection_id}/tags](../../riverhog/http-operations/get-v1-collections-collection-id-tags.md)
- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [GET /v1/retrieval-jobs/{job_id}/content](../../riverhog/http-operations/get-v1-retrieval-jobs-job-id-content.md)
- [GET /v1/retrieval-jobs/{job_id}](../../riverhog/http-operations/get-v1-retrieval-jobs-job-id.md)
- [GET /v1/retrieval-plans/{plan_id}/files](../../riverhog/http-operations/get-v1-retrieval-plans-plan-id-files.md)
- [POST /v1/retrieval-jobs/{job_id}/ack](../../riverhog/http-operations/post-v1-retrieval-jobs-job-id-ack.md)
- [POST /v1/retrieval-jobs/{job_id}/renew](../../riverhog/http-operations/post-v1-retrieval-jobs-job-id-renew.md)
- [POST /v1/retrieval-jobs](../../riverhog/http-operations/post-v1-retrieval-jobs.md)
- [POST /v1/retrieval-plans/{plan_id}/advance](../../riverhog/http-operations/post-v1-retrieval-plans-plan-id-advance.md)
- [POST /v1/retrieval-plans](../../riverhog/http-operations/post-v1-retrieval-plans.md)
- [riverhog_client.ApiClient.acknowledge_retrieval_job](../../riverhog-client/python/riverhog-client-apiclient-acknowledge-retrieval-job.md)
- [riverhog_client.ApiClient.advance_retrieval_plan](../../riverhog-client/python/riverhog-client-apiclient-advance-retrieval-plan.md)
- [riverhog_client.ApiClient.create_retrieval_job](../../riverhog-client/python/riverhog-client-apiclient-create-retrieval-job.md)
- [riverhog_client.ApiClient.download_retrieval_file](../../riverhog-client/python/riverhog-client-apiclient-download-retrieval-file.md)
- [riverhog_client.ApiClient.get_collection](../../riverhog-client/python/riverhog-client-apiclient-get-collection.md)
- [riverhog_client.ApiClient.get_portable_collection_inventory](../../riverhog-client/python/riverhog-client-apiclient-get-portable-collection-inventory.md)
- [riverhog_client.ApiClient.get_retrieval_job](../../riverhog-client/python/riverhog-client-apiclient-get-retrieval-job.md)
- [riverhog_client.ApiClient.list_collection_tags](../../riverhog-client/python/riverhog-client-apiclient-list-collection-tags.md)
- [riverhog_client.ApiClient.list_retrieval_plan_files](../../riverhog-client/python/riverhog-client-apiclient-list-retrieval-plan-files.md)
- [riverhog_client.ApiClient.plan_retrieval](../../riverhog-client/python/riverhog-client-apiclient-plan-retrieval.md)
- [riverhog_client.ApiClient.renew_retrieval_job](../../riverhog-client/python/riverhog-client-apiclient-renew-retrieval-job.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-fea31fec66"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-d82fa7b945"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/local.py::repair](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/local.py#L1124)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/local/commands/repair/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/repair/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/repair/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/repair/name`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/repair/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/repair/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/repair/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/repair/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/repair/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/repair/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/repair/name`

<!-- exact-contract-value: 15b490ee176bfe3f1065942fd75feec8e02c2d9beeaca063b55a973f0da8c0e4 -->

```json
"repair"
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/repair/parameters`

<!-- exact-contract-value: 6d6f63e6cd222ae7a2b7a7bcbac898d6478aced320f5378d56f9d313ff59ebd7 -->

```json
[
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "wait",
    "nargs": 1,
    "options": [
      "--wait"
    ],
    "required": false,
    "secondary_options": [
      "--no-wait"
    ],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  },
  {
    "count": false,
    "default": "allow",
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "restore_policy",
    "nargs": 1,
    "options": [
      "--restore-policy"
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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/repair/result_contract`

<!-- exact-contract-value: 03854cf66e07fffd67ebaa38f1d08418be9ee14ca2101afcbea3ee83a0d7cf53 -->

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
  "identity": "a-riverhog-cli-result/local/repair/v1",
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
          "identity": "a-riverhog-cli-local-sync-result/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "materialized_files": {
                "minimum": 0,
                "type": "integer"
              },
              "restore_policy": {
                "enum": [
                  "allow",
                  "never"
                ]
              },
              "retrieval": {
                "type": "object"
              },
              "retrieval_id": {
                "type": "string"
              },
              "status": {
                "enum": [
                  "requested",
                  "ready",
                  "cache-miss",
                  "current",
                  "materialized"
                ]
              },
              "unavailable_files": {
                "minimum": 0,
                "type": "integer"
              }
            },
            "required": [
              "status",
              "materialized_files"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/repair/terminating_controls`

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
