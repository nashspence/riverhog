# a-riverhog-cli archive copy watch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-archive-copy-watch:d59ab9fbac -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-f9db9554e0"></a>Parser name: `watch`
- <a id="s-54d2db1098"></a>Extra arguments at this parser: rejected.
- <a id="s-bf0278299a"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-f5feee6d78"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-039734ecee"></a>`selector`<br>`selector` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-56f97a8efa"></a>`interval`<br>`--interval` | optional option; 1 value | float range; minimum=`0.1` (inclusive); outside range: reject | `1`<br>Env: `null` |
| <a id="s-6e442859c0"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5d0e716c97"></a>`help` | <a id="s-ab7772b6c0"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-4967415224"></a>`0` | <a id="s-5e90fbcb1a"></a>`"noncontractual-framework-help"` | <a id="s-1ca609abf8"></a>`"empty"` |

### Result and failure contract

- <a id="s-54a6009e73"></a>Result identity: `a-riverhog-cli-result/archive/copy/watch/v1`
- <a id="s-1681a97854"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-3ff133d919"></a>Structured output: `optional-json`
- <a id="s-048dfe4920"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-7808795396"></a>`completed` | <a id="s-dbf9936b25"></a>`{"kind":"command-completed"}` | <a id="s-c4d6286a50"></a>`0` | <a id="s-d30ddd2ece"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_archive_copy_job response 200](../../riverhog/http-operations/get-v1-archive-copies-collection-id-destination-store.md#s-7ffcd84564) | <a id="s-bf52bc95df"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-15237d5e35"></a>`usage` | <a id="s-2f56529dd4"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-cc34b52fe5"></a>`2` | <a id="s-2556e2b345"></a>all: `"empty"` | <a id="s-d48f7b6373"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-1a5e32678d"></a>`operational` | <a id="s-85905a82f7"></a>`{"kind":"application-error"}` | <a id="s-302a25d0fa"></a>`1` | <a id="s-60cecb27db"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-b9faf0a563"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |
| <a id="s-a00573ca44"></a>`terminal-job-failure` | <a id="s-4ac98cfb9b"></a>`{"kind":"archive-copy-state","state":"failed"}` | <a id="s-c29d65c630"></a>`1` | <a id="s-7d6c0776eb"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_archive_copy_job response 200](../../riverhog/http-operations/get-v1-archive-copies-collection-id-destination-store.md#s-7ffcd84564) | <a id="s-609907f498"></a>all: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --interval](#s-56f97a8efa) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-6e442859c0) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter selector](#s-039734ecee) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/archive/copies/{collection_id}/{destination_store}](../../riverhog/http-operations/get-v1-archive-copies-collection-id-destination-store.md)
- [riverhog_client.ApiClient.get_archive_copy_job](../../riverhog-client/python/riverhog-client-apiclient-get-archive-copy-job.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-244c96a0cb"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-de55f535bd"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::archive\_copy\_watch\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L3095)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/watch/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/watch/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/watch/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/watch/name`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/watch/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/watch/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/watch/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/watch/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/watch/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/watch/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/watch/name`

<!-- exact-contract-value: 73da76bff71a604995ddd94e223ffa8b7c171b54e0a953c0fb794ac85a61534b -->

```json
"watch"
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/watch/parameters`

<!-- exact-contract-value: 75dc4e03bb6d35db32eca142a6f52a9faf8f7ae59353723fd6931d1b7cc53e02 -->

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
    "default": 1,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "interval",
    "nargs": 1,
    "options": [
      "--interval"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "clamp": false,
      "class": "typer._click.types.FloatRange",
      "max_open": false,
      "min_open": false,
      "minimum": 0.1,
      "name": "float range"
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

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/watch/result_contract`

<!-- exact-contract-value: da38cadc2106313f88f4c57a6ac7852276e9ee2a07f0a5d17bc06bf14dedcce0 -->

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
    },
    {
      "exit_status": 1,
      "id": "terminal-job-failure",
      "selected_by": {
        "kind": "archive-copy-state",
        "state": "failed"
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
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "a-riverhog-cli-result/archive/copy/watch/v1",
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

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy/commands/watch/terminating_controls`

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
