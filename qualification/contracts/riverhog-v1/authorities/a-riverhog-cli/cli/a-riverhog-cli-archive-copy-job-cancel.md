# a-riverhog-cli archive copy-job cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-archive-copy-job-cancel:9f827727d1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-6f0a360506"></a>Parser name: `cancel`
- <a id="s-964883c3bd"></a>Extra arguments at this parser: rejected.
- <a id="s-b7e51cf9ed"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-8b9145c69c"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-278a27e953"></a>`selector`<br>`selector` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-56191de51f"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-07962a7d03"></a>`help` | <a id="s-bd513c1756"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-e1d4591cdc"></a>`0` | <a id="s-92a44eaa5e"></a>`"noncontractual-framework-help"` | <a id="s-c98ed3354e"></a>`"empty"` |

### Result and failure contract

- <a id="s-c444c58eb8"></a>Result identity: `a-riverhog-cli-result/archive/copy-job/cancel/v1`
- <a id="s-57516c0996"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-ff85d36c7f"></a>Structured output: `optional-json`
- <a id="s-1b7bc69fd7"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-29aecffa08"></a>`completed` | <a id="s-acaf1fc672"></a>`{"kind":"command-completed"}` | <a id="s-33f7de4fd7"></a>`0` | <a id="s-b0511d0da6"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP cancel_archive_copy_job response 200](../../riverhog/http-operations/delete-v1-archive-copy-jobs-collection-id-destination-store.md#s-4e13233952) | <a id="s-44e2840bb3"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-cf0a6f31e7"></a>`usage` | <a id="s-d81ac3e03b"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-1b3ddfaaf9"></a>`2` | <a id="s-1dc6fd97bd"></a>all: `"empty"` | <a id="s-faebcd3288"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-907a347a17"></a>`operational` | <a id="s-b1b110ab6b"></a>`{"kind":"application-error"}` | <a id="s-5d2a7716bd"></a>`1` | <a id="s-c5acb17780"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-033000b094"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-56191de51f) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter selector](#s-278a27e953) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [DELETE /v1/archive/copy-jobs/{collection_id}/{destination_store}](../../riverhog/http-operations/delete-v1-archive-copy-jobs-collection-id-destination-store.md)
- [riverhog_client.ApiClient.cancel_archive_copy_job](../../riverhog-client/python/riverhog-client-apiclient-cancel-archive-copy-job.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-2469b495f6"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-80f5b0f01c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::archive\_copy\_cancel\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L3132)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/cancel/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/cancel/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/cancel/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/cancel/name`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/cancel/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/cancel/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/cancel/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/cancel/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/cancel/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/cancel/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/cancel/name`

<!-- exact-contract-value: 5a83111e44703c9dd7431bae2754317bb495994fbf736cf7739842ded4dcbc20 -->

```json
"cancel"
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/cancel/parameters`

<!-- exact-contract-value: 6b32bc56c11c2cdafb747d7a7aa740468c2f614b5b1d0e2588fb031d746e52f9 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "selector",
    "nargs": 1,
    "options": [
      "selector"
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

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/cancel/result_contract`

<!-- exact-contract-value: dbaa55c28b008ad13fcbfced5b87d2361925a33e948177f9626709243e351d23 -->

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
  "identity": "a-riverhog-cli-result/archive/copy-job/cancel/v1",
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
          "method": "DELETE",
          "operation_id": "cancel_archive_copy_job",
          "path": "/v1/archive/copy-jobs/{collection_id}/{destination_store}",
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

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/cancel/terminating_controls`

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
