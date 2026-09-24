# a-riverhog-cli collection provenance export

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-provenance-export:cda72ba975 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-70621e6170"></a>Parser name: `export`
- <a id="s-ef9cea0114"></a>Extra arguments at this parser: rejected.
- <a id="s-73faad1843"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-3319e7d932"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-0ac5441af3"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-b0a4184b30"></a>`journal_id`<br>`journal_id` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-e4c87d0ab8"></a>`output`<br>`--output`, `-o` | required option; 1 value | path; existence not required; regular files allowed; directories allowed; access checks on existing paths: read; resolve absolute path and symlinks: no; dash uses normal path checks | not recorded<br>Env: `null` |
| <a id="s-52519291a3"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-1e187a9719"></a>`help` | <a id="s-e72166a85b"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-1fb32e34d7"></a>`0` | <a id="s-04bd084332"></a>`"noncontractual-framework-help"` | <a id="s-287ec9a6a6"></a>`"empty"` |

### Result and failure contract

- <a id="s-1b77b7bd1c"></a>Result identity: `a-riverhog-cli-result/collection/provenance/export/v1`
- <a id="s-6452bd80e7"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-ed3e7bef55"></a>Structured output: `optional-json`
- <a id="s-86cee91bc8"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-25906a24ce"></a>`completed` | <a id="s-897048cd5a"></a>`{"kind":"command-completed"}` | <a id="s-04f3a76a82"></a>`0` | <a id="s-49aa8eccfc"></a>human: `"noncontractual-presentation-of-command-result"`; json: [a-riverhog-cli-provenance-journal-export/v1](#s-1d9005f667) | <a id="s-fe964634e0"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-92669eb99e"></a>`usage` | <a id="s-bfe1d0f934"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-d15f60f348"></a>`2` | <a id="s-a146eca638"></a>all: `"empty"` | <a id="s-a1899b2fd2"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-eaac5b70ff"></a>`operational` | <a id="s-4755777c57"></a>`{"kind":"application-error"}` | <a id="s-0a083b135b"></a>`1` | <a id="s-f785eaae47"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-11ceed68a0"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-1d9005f667"></a>`a-riverhog-cli-provenance-journal-export/v1`

Applies to: completed · stdout (json).

<a id="s-21a1c12fc4"></a>

- <a id="s-e2a4ab2f62"></a>`type`: `"object"`
- <a id="s-e6c0a0402b"></a>`additionalProperties`: `false`
- <a id="s-26e71f6e49"></a>`required`: `["collection_id","journal_id","output","bytes","sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d83e263d38"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-d356ade150"></a>`collection_id` | yes | type="integer"; minimum=1 |  |
| <a id="s-089fdab216"></a>`journal_id` | yes | type="string" |  |
| <a id="s-09c5b2858e"></a>`output` | yes | type="string" |  |
| <a id="s-c1ab53463d"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-0ac5441af3) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter journal_id](#s-b0a4184b30) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-52519291a3) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --output](#s-e4c87d0ab8) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/journals/{journal_id}](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-journals-journal-id.md)
- [riverhog_client.ApiClient.stream_collection_provenance_journal](../../riverhog-client/python/riverhog-client-apiclient-stream-collection-provenance-journal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-9d4ebaa3e5"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-be5c6c47c4"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::provenance\_export\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2690)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/export/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/export/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/export/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/export/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/export/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/export/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/export/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/export/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/export/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/export/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/export/name`

<!-- exact-contract-value: 3491337573000235960ad0689fabc5fc85f50d5718f2a62c9f46fbe3047f2a55 -->

```json
"export"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/export/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/export/result_contract`

<!-- exact-contract-value: bd090e4927dbeb6e636a655bb37fe8eca0bd834b1ed9c4c5c7d0aaaeae6279fa -->

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
  "identity": "a-riverhog-cli-result/collection/provenance/export/v1",
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
          "identity": "a-riverhog-cli-provenance-journal-export/v1",
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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/export/terminating_controls`

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
