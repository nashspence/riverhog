# piggity collection provenance export

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance-export:48f0d22d07 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-002c4f842a"></a>Parser name: `export`
- <a id="s-b1d2dd1ddd"></a>Extra arguments at this parser: rejected.
- <a id="s-d24eb9c840"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-3957f61c27"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-546fc23aad"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-918685ae19"></a>`journal_id`<br>`journal_id` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-23253560e6"></a>`output`<br>`--output`, `-o` | required option; 1 value | path; existence not required; regular files allowed; directories allowed; access checks on existing paths: read; resolve absolute path and symlinks: no; dash uses normal path checks | not recorded<br>Env: `null` |
| <a id="s-4168b25082"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-b9062a41d1"></a>`help` | <a id="s-6ae652ee1a"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-e73019dc23"></a>`0` | <a id="s-cde9e6b05d"></a>`"noncontractual-framework-help"` | <a id="s-029ac51271"></a>`"empty"` |

### Result and failure contract

- <a id="s-2bbaaf4c9c"></a>Result identity: `piggity-cli-result/collection/provenance/export/v1`
- <a id="s-6c9a98688f"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-8bd4c297d8"></a>Structured output: `optional-json`
- <a id="s-3008008bb9"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-54c365a877"></a>`completed` | <a id="s-9eb5f8121b"></a>`{"kind":"command-completed"}` | <a id="s-3c77526821"></a>`0` | <a id="s-a3a0a70fd5"></a>human: `"noncontractual-presentation-of-command-result"`; json: [piggity-provenance-journal-export/v1](#s-b542d66ff6) | <a id="s-e9d03a6eb5"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-d77f6a8f47"></a>`usage` | <a id="s-57925c97aa"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-9dc652abf3"></a>`2` | <a id="s-b2d941374b"></a>all: `"empty"` | <a id="s-d0e0ed8df9"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-cb2d3355cc"></a>`operational` | <a id="s-810fd80b39"></a>`{"kind":"application-error"}` | <a id="s-4bbf5dc3b2"></a>`1` | <a id="s-c9f2314d7e"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-b55e01adc5"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-b542d66ff6"></a>`piggity-provenance-journal-export/v1`

Applies to: completed · stdout (json).

<a id="s-5b5ef7595a"></a>

- <a id="s-93ea5d06e9"></a>`type`: `"object"`
- <a id="s-fde6aed09f"></a>`additionalProperties`: `false`
- <a id="s-2f9af96650"></a>`required`: `["collection_id","journal_id","output","bytes","sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3d6696a103"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-c0925f8b8f"></a>`collection_id` | yes | type="integer"; minimum=1 |  |
| <a id="s-54011914e4"></a>`journal_id` | yes | type="string" |  |
| <a id="s-d6f848ec37"></a>`output` | yes | type="string" |  |
| <a id="s-ef892ce86a"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-546fc23aad) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter journal_id](#s-918685ae19) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-4168b25082) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --output](#s-23253560e6) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/journals/{journal_id}](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-journals-journal-id.md)
- [riverhog_client.ApiClient.stream_collection_provenance_journal](../../riverhog-client/python/riverhog-client-apiclient-stream-collection-provenance-journal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-549f52d451"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-311ee39898"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources/authorities.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::provenance\_export\_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2675)

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/allow_extra_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/result_contract`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/name`

<!-- exact-contract-value: 3491337573000235960ad0689fabc5fc85f50d5718f2a62c9f46fbe3047f2a55 -->

```json
"export"
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/parameters`

<!-- exact-contract-value: 172eb38024f7f529a857232b400b239af1b17988a1e4b4804028b925f780d7cc -->

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
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "journal_id",
    "nargs": 1,
    "options": [
      "journal_id"
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
    "name": "output",
    "nargs": 1,
    "options": [
      "--output",
      "-o"
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

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/result_contract`

<!-- exact-contract-value: c2ba458a610e115d8e3d76b8d593cc72c141855dc693caa007866036bcbfc4b0 -->

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
  "identity": "piggity-cli-result/collection/provenance/export/v1",
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
          "identity": "piggity-provenance-journal-export/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "bytes": {
                "minimum": 0,
                "type": "integer"
              },
              "collection_id": {
                "minimum": 1,
                "type": "integer"
              },
              "journal_id": {
                "type": "string"
              },
              "output": {
                "type": "string"
              },
              "sha256": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              }
            },
            "required": [
              "collection_id",
              "journal_id",
              "output",
              "bytes",
              "sha256"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/export/terminating_controls`

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
