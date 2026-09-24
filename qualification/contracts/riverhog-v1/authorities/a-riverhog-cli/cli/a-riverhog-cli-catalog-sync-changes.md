# a-riverhog-cli catalog-sync changes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-catalog-sync-changes:b95a47e275 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-886d565a26"></a>Parser name: `changes`
- <a id="s-8bb5bd3919"></a>Extra arguments at this parser: rejected.
- <a id="s-503da9e777"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-a2b4363be0"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-237b5bc7ac"></a>`cursor`<br>`--cursor` | required option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-eff2367a0a"></a>`limit`<br>`--limit` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `100`<br>Env: `null` |
| <a id="s-2f5640aa93"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-a0806d5443"></a>`help` | <a id="s-cb8c4a1d77"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-42dbfde617"></a>`0` | <a id="s-357d6fa9ef"></a>`"noncontractual-framework-help"` | <a id="s-723d0f46eb"></a>`"empty"` |

### Result and failure contract

- <a id="s-25fde49a65"></a>Result identity: `a-riverhog-cli-result/catalog-sync/changes/v1`
- <a id="s-c7792ec465"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-8654302b2d"></a>Structured output: `optional-json`
- <a id="s-baebaad3d6"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6bd49ebed5"></a>`completed` | <a id="s-d323afb6c0"></a>`{"kind":"command-completed"}` | <a id="s-95c9f3a719"></a>`0` | <a id="s-f662ccfe1a"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP list_catalog_sync_changes response 200](../../riverhog/http-operations/get-v1-catalog-sync-changes.md#s-965d91e8aa) | <a id="s-b8234a9bef"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-ca48e34dce"></a>`usage` | <a id="s-a45be7913c"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-4379c1848a"></a>`2` | <a id="s-7996f8454a"></a>all: `"empty"` | <a id="s-cf582eecbb"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-81b2ded775"></a>`operational` | <a id="s-34ecb81749"></a>`{"kind":"application-error"}` | <a id="s-4702eb998d"></a>`1` | <a id="s-f6135d5c20"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-9cf68bd6ec"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --cursor](#s-237b5bc7ac) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-2f5640aa93) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --limit](#s-eff2367a0a) | `value · cli-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --limit](#s-eff2367a0a) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/catalog-sync/changes](../../riverhog/http-operations/get-v1-catalog-sync-changes.md)
- [riverhog_client.ApiClient.list_catalog_sync_changes](../../riverhog-client/python/riverhog-client-apiclient-list-catalog-sync-changes.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-0d4ef01316"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-c9d24c2e6f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::catalog\_sync\_changes\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L815)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/changes/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/changes/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/changes/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/changes/name`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/changes/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/changes/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/changes/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/changes/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/changes/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/changes/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/changes/name`

<!-- exact-contract-value: 1914b3c81072ec685b513a4497b8db642b0328afb8aeaede6dd78a6311879dbe -->

```json
"changes"
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/changes/parameters`

<!-- exact-contract-value: 853a7f08f408c63232b733e88e9960df9f2429d90d8e287ab5d12ac28996ccd0 -->

```json
[
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "cursor",
    "nargs": 1,
    "options": [
      "--cursor"
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
    "default": 100,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "limit",
    "nargs": 1,
    "options": [
      "--limit"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "clamp": false,
      "class": "typer._click.types.IntRange",
      "max_open": false,
      "maximum": 100,
      "min_open": false,
      "minimum": 1,
      "name": "integer range"
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

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/changes/result_contract`

<!-- exact-contract-value: fdca729793dbe16ae03bb2fbeecbfbbc0c4c1efbd6642cb5998a857f6d80c2c5 -->

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
  "identity": "a-riverhog-cli-result/catalog-sync/changes/v1",
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
          "operation_id": "list_catalog_sync_changes",
          "path": "/v1/catalog-sync/changes",
          "schema": {
            "$ref": "#/components/schemas/CatalogSyncChangePage"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/changes/terminating_controls`

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
