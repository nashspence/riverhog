# piggity archive copy start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-archive-copy-start:9a14f4a9a1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-47f6ebe872"></a>Parser name: `start`
- <a id="s-9394122b27"></a>Extra arguments at this parser: rejected.
- <a id="s-c56fb61279"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-ae70450bb7"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-46f197ecd8"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded |
| <a id="s-4494707e36"></a>`destination_store`<br>`--to` | required option; 1 value | text | not recorded |
| <a id="s-4d08291539"></a>`source_store`<br>`--from` | optional option; 1 value | text | not recorded |
| <a id="s-57c4862dba"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-bc5679ede3"></a>`help` | <a id="s-c0ba170306"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-e975c2c50d"></a>`0` | <a id="s-0e2c4793a9"></a>`"noncontractual-framework-help"` | <a id="s-0218138745"></a>`"empty"` |

### Result and failure contract

- <a id="s-871d696c21"></a>Result identity: `piggity-cli-result/archive/copy/start/v1`
- <a id="s-d625961be0"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-9aa3fe17a0"></a>Structured output: `optional-json`
- <a id="s-a71ce0144d"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5e6aecaa4f"></a>`completed` | <a id="s-4eaf853c5b"></a>`{"kind":"command-completed"}` | <a id="s-b70e3eb72c"></a>`0` | <a id="s-70b315d303"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP create_or_resume_archive_copy response 200](../../riverhog/http-operations/post-v1-archive-copies.md#s-39d8493e84) | <a id="s-b096b0b70f"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-45d0107ee7"></a>`usage` | <a id="s-8a6722c886"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-f19906c148"></a>`2` | <a id="s-3d7f6ea8f2"></a>all: `empty` | <a id="s-5e81abfb29"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-58949d1fcd"></a>`operational` | <a id="s-e7f57de25e"></a>`{"kind":"application-error"}` | <a id="s-b3060936cb"></a>`1` | <a id="s-f44be5715e"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-844d1d5a77"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-46f197ecd8) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --to](#s-4494707e36) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-57c4862dba) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --from](#s-4d08291539) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [POST /v1/archive/copies](../../riverhog/http-operations/post-v1-archive-copies.md)
- [riverhog_client.ApiClient.create_or_resume_archive_copy](../../riverhog-client/python/riverhog-client-apiclient-create-or-resume-archive-copy.md)

## Governing policies

- <a id="pa-ff4020f583"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-5cdce11599"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::archive_copy_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2969)

### Machine authority

- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/start/allow_extra_args`
- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/start/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/start/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/start/name`
- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/start/parameters`
- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/start/result_contract`
- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/start/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/start/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/start/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/start/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/start/name`

<!-- exact-contract-value: a92ae9615600f7f0bcb0edf9703b379c163bef33ed749ae40c48a0830d4ab6ae -->

```json
"start"
```

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/start/parameters`

<!-- exact-contract-value: c7ff3ed6ced0e984db2acb296727ee8504a8533e7af6ec8deeb3b7d42775493e -->

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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "destination_store",
    "nargs": 1,
    "options": [
      "--to"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "source_store",
    "nargs": 1,
    "options": [
      "--from"
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

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/start/result_contract`

<!-- exact-contract-value: 20556465163ad463ba9d1e5e7902c7c29eabd0e9002349a493983c07fb5a73c7 -->

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
  "identity": "piggity-cli-result/archive/copy/start/v1",
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
          "method": "POST",
          "operation_id": "create_or_resume_archive_copy",
          "path": "/v1/archive/copies",
          "schema": {
            "$ref": "#/components/schemas/ArchiveCopyJobOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/start/terminating_controls`

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
