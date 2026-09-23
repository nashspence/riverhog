# a-riverhog-cli archive copy start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-archive-copy-start:47e7456f78 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-eff8d9bbff"></a>Parser name: `start`
- <a id="s-2cdef0b9cf"></a>Extra arguments at this parser: rejected.
- <a id="s-0723647213"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-ac2c513e70"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-14b31654f7"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-8fab94b459"></a>`destination_store`<br>`--to` | required option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-2b5701c1ff"></a>`source_store`<br>`--from` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-ee7eead1a3"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-b3e02268f3"></a>`help` | <a id="s-e6defad266"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c6abab86f7"></a>`0` | <a id="s-9395ae40ab"></a>`"noncontractual-framework-help"` | <a id="s-c8be4bc282"></a>`"empty"` |

### Result and failure contract

- <a id="s-59cbd3277c"></a>Result identity: `a-riverhog-cli-result/archive/copy/start/v1`
- <a id="s-15d2562864"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-7d7e6aebcb"></a>Structured output: `optional-json`
- <a id="s-21a7893506"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-693a5f9f2e"></a>`completed` | <a id="s-48de68c201"></a>`{"kind":"command-completed"}` | <a id="s-6d47a3cb51"></a>`0` | <a id="s-71ca78cab8"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP create_or_resume_archive_copy response 200](../../riverhog/http-operations/post-v1-archive-copies.md#s-39d8493e84) | <a id="s-9341fc1443"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-e9c935abaf"></a>`usage` | <a id="s-30df6bd441"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-3b02ac4aec"></a>`2` | <a id="s-8f088582d4"></a>all: `"empty"` | <a id="s-b2d2e5770a"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-1cbe356f23"></a>`operational` | <a id="s-e5fcfa4894"></a>`{"kind":"application-error"}` | <a id="s-0420f74c71"></a>`1` | <a id="s-5c4d08dc5d"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-61121ca160"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-14b31654f7) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --to](#s-8fab94b459) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-ee7eead1a3) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --from](#s-2b5701c1ff) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [POST /v1/archive/copies](../../riverhog/http-operations/post-v1-archive-copies.md)
- [riverhog_client.ApiClient.create_or_resume_archive_copy](../../riverhog-client/python/riverhog-client-apiclient-create-or-resume-archive-copy.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-dca44e33c2"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-91fe11d05c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::archive\_copy\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2984)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/start/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/start/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/start/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/start/name`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/start/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/start/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/start/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/start/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/start/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/start/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/start/name`

<!-- exact-contract-value: a92ae9615600f7f0bcb0edf9703b379c163bef33ed749ae40c48a0830d4ab6ae -->

```json
"start"
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/start/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/start/result_contract`

<!-- exact-contract-value: 2f6058040815e04aee47ee4700d182f92d5e64cf63a4c8954b17fbee74734fdf -->

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
  "identity": "a-riverhog-cli-result/archive/copy/start/v1",
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

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/start/terminating_controls`

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
