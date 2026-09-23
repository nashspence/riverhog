# piggity local sync

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-sync:65ec1ddd1f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d3e6816ced"></a>Parser name: `sync`
- <a id="s-e65e5019b8"></a>Extra arguments at this parser: rejected.
- <a id="s-21c8520ba8"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-de03d49625"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-6368e8bf41"></a>`wait`<br>`--wait`, alternate: `--no-wait` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-78b95fe0d7"></a>`restore_policy`<br>`--restore-policy` | optional option; 1 value | text | `"allow"`<br>Env: `null` |
| <a id="s-b7ec88d2f5"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-f4ddb48dd2"></a>`help` | <a id="s-89937b165e"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-1a5e83c2e3"></a>`0` | <a id="s-7704616899"></a>`"noncontractual-framework-help"` | <a id="s-0243c1df05"></a>`"empty"` |

### Result and failure contract

- <a id="s-166ce3e341"></a>Result identity: `piggity-cli-result/local/sync/v1`
- <a id="s-b04919f2dd"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-15c2511b38"></a>Structured output: `optional-json`
- <a id="s-dab1add368"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a88a58b788"></a>`completed` | <a id="s-27bddeb10f"></a>`{"kind":"command-completed"}` | <a id="s-ad38c1ecbd"></a>`0` | <a id="s-d1c0d802ff"></a>human: `"noncontractual-presentation-of-command-result"`; json: [piggity-local-sync-result/v1](#s-d7b67dfc92) | <a id="s-6a0c8e1296"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-2c68626b32"></a>`usage` | <a id="s-a7305c6269"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-6122ac8b73"></a>`2` | <a id="s-969d817e85"></a>all: `"empty"` | <a id="s-f30185b3b5"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-306e62a3ab"></a>`operational` | <a id="s-1a8da6175b"></a>`{"kind":"application-error"}` | <a id="s-4ef01f8631"></a>`1` | <a id="s-5cdfee6996"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-dc0e16944e"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-d7b67dfc92"></a>`piggity-local-sync-result/v1`

Applies to: completed · stdout (json).

<a id="s-ddca095d04"></a>

- <a id="s-22047c161e"></a>`type`: `"object"`
- <a id="s-80d87dc9e2"></a>`additionalProperties`: `false`
- <a id="s-d6103101b8"></a>`required`: `["status","materialized_files"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0e02208654"></a>`materialized_files` | yes | type="integer"; minimum=0 |  |
| <a id="s-f861347c2e"></a>`restore_policy` | no | enum=["allow","never"] |  |
| <a id="s-d64ee0a41d"></a>`retrieval` | no | type="object" |  |
| <a id="s-7ca6d2321f"></a>`retrieval_id` | no | type="string" |  |
| <a id="s-e08d0c1d13"></a>`status` | yes | enum=["requested","ready","cache-miss","current","materialized"] |  |
| <a id="s-5a160dda3d"></a>`unavailable_files` | no | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-b7ec88d2f5) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --restore-policy](#s-78b95fe0d7) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --wait](#s-6368e8bf41) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

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

- <a id="pa-0ffe3dba51"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-a0c69d47c6"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources/authorities.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/local.py::sync](../../../../../../reference/riverhog/applications/piggity/src/piggity/local.py#L1101)

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/sync/allow_extra_args`
- `/external_contract/cli/piggity/commands/local/commands/sync/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/local/commands/sync/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/local/commands/sync/name`
- `/external_contract/cli/piggity/commands/local/commands/sync/parameters`
- `/external_contract/cli/piggity/commands/local/commands/sync/result_contract`
- `/external_contract/cli/piggity/commands/local/commands/sync/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/sync/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/sync/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/local/commands/sync/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/sync/name`

<!-- exact-contract-value: 5cfa1a73f845669922af31371796703a709a5489a6db9091aaf64d8d905aedde -->

```json
"sync"
```

### `/external_contract/cli/piggity/commands/local/commands/sync/parameters`

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

### `/external_contract/cli/piggity/commands/local/commands/sync/result_contract`

<!-- exact-contract-value: 4de307b9b5085335df367f92a865a6f800ffad9d7ae73f25856dda07a91b3cd2 -->

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
  "identity": "piggity-cli-result/local/sync/v1",
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
          "identity": "piggity-local-sync-result/v1",
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

### `/external_contract/cli/piggity/commands/local/commands/sync/terminating_controls`

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
