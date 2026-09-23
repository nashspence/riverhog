# a-riverhog-cli collection upload start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-upload-start:d3cdefa1f3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-04b9ade5e4"></a>Parser name: `start`
- <a id="s-ef8c962064"></a>Extra arguments at this parser: rejected.
- <a id="s-a556d69682"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-7a9e5c21e2"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-550fa1beb6"></a>`root`<br>`root` | required positional; 1 value | path; existence not required; regular files allowed; directories allowed; access checks on existing paths: read; resolve absolute path and symlinks: no; dash uses normal path checks | not recorded<br>Env: `null` |
| <a id="s-54765a73dd"></a>`idempotency_key`<br>`--idempotency-key` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-ba892a4d2d"></a>`archive_store`<br>`--archive-store` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-d7bca71bef"></a>`description`<br>`--description` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-95e4104c03"></a>`tag`<br>`--tag` | optional option; 1 value; collects repeats; no declared occurrence maximum | text | not recorded<br>Env: `null` |
| <a id="s-cd32f431c1"></a>`provenance`<br>`--provenance` | optional option; 1 value | path; existence not required; regular files allowed; directories allowed; access checks on existing paths: read; resolve absolute path and symlinks: no; dash uses normal path checks | not recorded<br>Env: `null` |
| <a id="s-3c8c2e70e6"></a>`omit_provenance`<br>`--omit-provenance` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-16fb5d6abe"></a>`provenance_observer`<br>`--provenance-observer` | optional option; 1 value | text | not recorded<br>Env: `"A_RIVERHOG_CLI_PROVENANCE_OBSERVER"` |
| <a id="s-3e642da5a2"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-001a52eaf4"></a>`dry_run`<br>`--dry-run` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-66ff182284"></a>`help` | <a id="s-3237a540e1"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-a286c30f90"></a>`0` | <a id="s-019764220e"></a>`"noncontractual-framework-help"` | <a id="s-d0331c3afa"></a>`"empty"` |

### Result and failure contract

- <a id="s-54a0de0d30"></a>Result identity: `a-riverhog-cli-result/collection/upload/start/v1`
- <a id="s-1495bc5203"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-a2be50e9be"></a>Structured output: `optional-json`
- <a id="s-bf5a07c967"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-8c6fa1eb95"></a>`completed` | <a id="s-41702dd6c5"></a>`{"kind":"command-completed"}` | <a id="s-e9ea7af25e"></a>`0` | <a id="s-1e1d2d3f22"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_collection_upload_session response 200](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id.md#s-3478a57d08) | <a id="s-9eaa97686b"></a>all: `"noncontractual-progress"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0cf374bbc3"></a>`usage` | <a id="s-1f69e3b093"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-0d89937f5c"></a>`2` | <a id="s-712eff801e"></a>all: `"empty"` | <a id="s-503250f3ed"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-3d02017f68"></a>`operational` | <a id="s-8136c3cd3f"></a>`{"kind":"application-error"}` | <a id="s-f29d6ef6cb"></a>`1` | <a id="s-1c191cd822"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-961df9af26"></a>human: `"noncontractual-diagnostic-or-progress"`; json: `"noncontractual-progress"` |
| <a id="s-ec3f2ab723"></a>`custody-timeout` | <a id="s-f63b96260e"></a>`{"kind":"custody-deadline-expired","state":"not-finalized"}` | <a id="s-3ff724abe4"></a>`124` | <a id="s-a13f5879ba"></a>all: `"empty"` | <a id="s-3791f3309a"></a>all: `"noncontractual-progress"` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-cli"}; maximum=null; reason="no-declared-semantic-maximum"; source_constraint={"field":"multiple"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --tag](#s-95e4104c03) | `cardinality · occurrences · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --archive-store](#s-ba892a4d2d) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --description](#s-d7bca71bef) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --dry-run](#s-001a52eaf4) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --idempotency-key](#s-54765a73dd) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-3e642da5a2) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --omit-provenance](#s-3c8c2e70e6) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --provenance](#s-cd32f431c1) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --provenance-observer](#s-16fb5d6abe) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter root](#s-550fa1beb6) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --tag](#s-95e4104c03) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

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

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-eadffb8810"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-c3cff7cb75"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-701f9d935f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::upload\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2185)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/start/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/start/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/start/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/start/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/start/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/start/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/start/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/start/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/start/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/start/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/start/name`

<!-- exact-contract-value: a92ae9615600f7f0bcb0edf9703b379c163bef33ed749ae40c48a0830d4ab6ae -->

```json
"start"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/start/parameters`

<!-- exact-contract-value: 3e2bff14987e095133442c03734880f8aba186037c8b021d2363c52015bcae18 -->

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
      "allow_dash": false,
      "class": "typer.models.TyperPath",
      "dir_okay": true,
      "exists": false,
      "file_okay": true,
      "name": "path",
      "readable": true,
      "resolve_path": false,
      "writable": false
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
      "allow_dash": false,
      "class": "typer.models.TyperPath",
      "dir_okay": true,
      "exists": false,
      "file_okay": true,
      "name": "path",
      "readable": true,
      "resolve_path": false,
      "writable": false
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
    "envvar": "A_RIVERHOG_CLI_PROVENANCE_OBSERVER",
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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/start/result_contract`

<!-- exact-contract-value: 808a366a6cd275c3ae7a62b2cf3f5636e4625ec46244e48fa1a073f0566d5be0 -->

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
  "identity": "a-riverhog-cli-result/collection/upload/start/v1",
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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/start/terminating_controls`

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
