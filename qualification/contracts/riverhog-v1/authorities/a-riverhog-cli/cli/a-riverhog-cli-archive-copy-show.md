# a-riverhog-cli archive copy show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-archive-copy-show:6dfd8a22e4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-f0c8b17ab8"></a>Parser name: `show`
- <a id="s-7af67ceba0"></a>Extra arguments at this parser: rejected.
- <a id="s-e6e089fbfb"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-b019f2d7b8"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-996e9888e8"></a>`selector`<br>`selector` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-5cbdd7c802"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-b5aafa366f"></a>`help` | <a id="s-8bfc080d41"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-2440b2c87c"></a>`0` | <a id="s-42cbf38bf4"></a>`"noncontractual-framework-help"` | <a id="s-2a2a5dc446"></a>`"empty"` |

### Result and failure contract

- <a id="s-a54078ebd0"></a>Result identity: `a-riverhog-cli-result/archive/copy/show/v1`
- <a id="s-f9ef09885e"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-f4c59029e9"></a>Structured output: `optional-json`
- <a id="s-75dc61ceaa"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1dee473886"></a>`completed` | <a id="s-656129b2c3"></a>`{"kind":"command-completed"}` | <a id="s-2a63df484d"></a>`0` | <a id="s-0875a36117"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_archive_copy_job response 200](../../riverhog/http-operations/get-v1-archive-copies-collection-id-destination-store.md#s-7ffcd84564) | <a id="s-ae626bf95e"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c4bee25546"></a>`usage` | <a id="s-dab23124bc"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-221cb2c0ba"></a>`2` | <a id="s-163d8d1241"></a>all: `"empty"` | <a id="s-0c401e3900"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-4ba6f1731a"></a>`operational` | <a id="s-5e7115db64"></a>`{"kind":"application-error"}` | <a id="s-aed0e7ffb5"></a>`1` | <a id="s-8285d36254"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-4e026863ec"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-5cbdd7c802) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter selector](#s-996e9888e8) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/archive/copies/{collection_id}/{destination_store}](../../riverhog/http-operations/get-v1-archive-copies-collection-id-destination-store.md)
- [riverhog_client.ApiClient.get_archive_copy_job](../../riverhog-client/python/riverhog-client-apiclient-get-archive-copy-job.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-7fc186db0e"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-6568bcfd32"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::archive\_copy\_show\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L3059)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/show/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/show/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/show/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/show/name`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/show/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/show/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/show/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/show/result_contract`

<!-- exact-contract-value: a1cd02b142afc0c3f031b9433ac41dd3fb62f184871cdf681baff9e6449343f6 -->

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
  "identity": "a-riverhog-cli-result/archive/copy/show/v1",
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
          "method": "GET",
          "operation_id": "get_archive_copy_job",
          "path": "/v1/archive/copies/{collection_id}/{destination_store}",
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

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/show/terminating_controls`

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
