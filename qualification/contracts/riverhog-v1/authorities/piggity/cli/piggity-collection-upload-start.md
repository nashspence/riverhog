# piggity collection upload start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-upload-start:8a426b0dcf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-805df18e21"></a>Parser name: `start`

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-f8d43e8cc9"></a>`root`<br>`root` | required positional; 1 value | path | not recorded |
| <a id="s-41c9f19b64"></a>`idempotency_key`<br>`--idempotency-key` | optional option; 1 value | text | not recorded |
| <a id="s-50c5683cc5"></a>`archive_store`<br>`--archive-store` | optional option; 1 value | text | not recorded |
| <a id="s-d25200e1bc"></a>`description`<br>`--description` | optional option; 1 value | text | not recorded |
| <a id="s-02c396bde4"></a>`tag`<br>`--tag` | optional option; 1 value; collects repeats; no declared occurrence maximum | text | not recorded |
| <a id="s-d85e25fde3"></a>`provenance`<br>`--provenance` | optional option; 1 value | path | not recorded |
| <a id="s-0348cfd40b"></a>`omit_provenance`<br>`--omit-provenance` | optional option; 1 value | text | not recorded |
| <a id="s-598a4f2a05"></a>`provenance_observer`<br>`--provenance-observer` | optional option; 1 value | text | not recorded<br>Env: `"PIGGITY_PROVENANCE_OBSERVER"` |
| <a id="s-9bfdcc451f"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |
| <a id="s-0cf6176bfa"></a>`dry_run`<br>`--dry-run` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-6ea1f0da0c"></a>`help` | <a id="s-b2c6763b4a"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-b4f6223d61"></a>`0` | <a id="s-efe3fcd21f"></a>`"noncontractual-framework-help"` | <a id="s-7799839784"></a>`"empty"` |

### Result and failure contract

- <a id="s-c641d0aa0f"></a>Result identity: `piggity-cli-result/collection/upload/start/v1`
- <a id="s-35a62fc47d"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-62d43a9306"></a>Structured output: `optional-json`
- <a id="s-9d82d14a1d"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-d730df7a18"></a>`completed` | <a id="s-116a05c58c"></a>`{"kind":"command-completed"}` | <a id="s-8da7d6d337"></a>`0` | <a id="s-96b0ecc121"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP get_collection_upload_session response 200](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id.md#s-3478a57d08) | <a id="s-cd10769912"></a>all: `noncontractual-progress` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-bf21d6c0bd"></a>`usage` | <a id="s-89b791cd8a"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-0b9cafeddc"></a>`2` | <a id="s-af0f020720"></a>all: `empty` | <a id="s-dc49d5195d"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-50758df80a"></a>`operational` | <a id="s-8c078b501e"></a>`{"kind":"application-error"}` | <a id="s-647a0f71e3"></a>`1` | <a id="s-6843025be0"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-a441211ad2"></a>human: `noncontractual-diagnostic-or-progress`; json: `noncontractual-progress` |
| <a id="s-701a9ab0d7"></a>`custody-timeout` | <a id="s-537a092cd6"></a>`{"kind":"custody-deadline-expired","state":"not-finalized"}` | <a id="s-b595a7afb5"></a>`124` | <a id="s-79cbfef294"></a>all: `empty` | <a id="s-ef4c7379fd"></a>all: `noncontractual-progress` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"piggity"}; maximum=null; reason="no-declared-semantic-maximum"; source_constraint={"field":"multiple"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --tag](#s-02c396bde4) | `cardinality · occurrences · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --archive-store](#s-50c5683cc5) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --description](#s-d25200e1bc) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --dry-run](#s-0cf6176bfa) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --idempotency-key](#s-41c9f19b64) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-9bfdcc451f) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --omit-provenance](#s-0348cfd40b) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --provenance](#s-d85e25fde3) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --provenance-observer](#s-598a4f2a05) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter root](#s-f8d43e8cc9) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --tag](#s-02c396bde4) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id-volumes-volume-id-units-unit.md)
- [GET /v1/collection-upload-sessions/{collection_id}/work](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id-work.md)
- [GET /v1/collection-upload-sessions/{collection_id}](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id.md)
- [POST /v1/collection-upload-sessions/{collection_id}/files](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-files.md)
- [POST /v1/collection-upload-sessions/{collection_id}/tags](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-tags.md)
- [POST /v1/collection-upload-sessions/{collection_id}/complete](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-complete.md)
- [POST /v1/collection-upload-sessions/{collection_id}/raw-part-digests](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-raw-part-digests.md)
- [POST /v1/collection-upload-sessions](../../riverhog/http-operations/post-v1-collection-upload-sessions.md)
- [PUT /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}](../../riverhog/http-operations/put-v1-collection-upload-sessions-collection-id-volumes-volume-id-units-unit.md)
- [riverhog_client.ApiClient.acquire_collection_upload_session_work](../../riverhog-client/python/riverhog-client-apiclient-acquire-collection-upload-session-work.md)
- [riverhog_client.ApiClient.add_collection_upload_session_tags](../../riverhog-client/python/riverhog-client-apiclient-add-collection-upload-session-tags.md)
- [riverhog_client.ApiClient.complete_collection_upload_session](../../riverhog-client/python/riverhog-client-apiclient-complete-collection-upload-session.md)
- [riverhog_client.ApiClient.create_or_resume_collection_upload_session](../../riverhog-client/python/riverhog-client-apiclient-create-or-resume-collection-upload-session.md)
- [riverhog_client.ApiClient.get_collection_upload_session](../../riverhog-client/python/riverhog-client-apiclient-get-collection-upload-session.md)
- [riverhog_client.ApiClient.get_collection_upload_session_unit](../../riverhog-client/python/riverhog-client-apiclient-get-collection-upload-session-unit.md)
- [riverhog_client.ApiClient.put_collection_upload_session_unit](../../riverhog-client/python/riverhog-client-apiclient-put-collection-upload-session-unit.md)
- [riverhog_client.ApiClient.register_collection_upload_session_files](../../riverhog-client/python/riverhog-client-apiclient-register-collection-upload-session-files.md)
- [riverhog_client.ApiClient.register_collection_upload_session_raw_part_digests](../../riverhog-client/python/riverhog-client-apiclient-register-collection-upload-session-raw-part-digests.md)

## Governing policies

- <a id="pa-0a7f36f8db"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-f0c076c1a9"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-dc514bd006"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::upload_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2170)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/start/name`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/start/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/start/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/upload/commands/start/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/start/name`

<!-- exact-contract-value: a92ae9615600f7f0bcb0edf9703b379c163bef33ed749ae40c48a0830d4ab6ae -->

```json
"start"
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/start/parameters`

<!-- exact-contract-value: adb95542af19e382535840736acd6066011502a1227af6e297b20b882ba2c3be -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "root",
    "nargs": 1,
    "options": [
      "root"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "path"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "idempotency_key",
    "nargs": 1,
    "options": [
      "--idempotency-key"
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
    "name": "archive_store",
    "nargs": 1,
    "options": [
      "--archive-store"
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
    "name": "description",
    "nargs": 1,
    "options": [
      "--description"
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
    "multiple": true,
    "name": "tag",
    "nargs": 1,
    "options": [
      "--tag"
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
    "name": "provenance",
    "nargs": 1,
    "options": [
      "--provenance"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "path"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "omit_provenance",
    "nargs": 1,
    "options": [
      "--omit-provenance"
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
    "envvar": "PIGGITY_PROVENANCE_OBSERVER",
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "provenance_observer",
    "nargs": 1,
    "options": [
      "--provenance-observer"
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
  },
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "dry_run",
    "nargs": 1,
    "options": [
      "--dry-run"
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

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/start/result_contract`

<!-- exact-contract-value: 905ccbfea4ef0049ff3f4700ef0433eb8d6d60399aa1db02ccfcb5dfa804acee -->

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
        "human": "noncontractual-diagnostic-or-progress",
        "json": "noncontractual-progress"
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
    },
    {
      "exit_status": 124,
      "id": "custody-timeout",
      "selected_by": {
        "kind": "custody-deadline-expired",
        "state": "not-finalized"
      },
      "stderr": {
        "all": "noncontractual-progress"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "piggity-cli-result/collection/upload/start/v1",
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
        "all": "noncontractual-progress"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "application": "riverhog",
          "kind": "http-operation-response",
          "method": "GET",
          "operation_id": "get_collection_upload_session",
          "path": "/v1/collection-upload-sessions/{collection_id}",
          "schema": {
            "$ref": "#/components/schemas/CollectionUploadSessionOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/upload/commands/start/terminating_controls`

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
