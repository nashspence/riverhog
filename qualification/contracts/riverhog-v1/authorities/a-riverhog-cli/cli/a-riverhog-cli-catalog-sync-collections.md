# a-riverhog-cli catalog-sync collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-catalog-sync-collections:447c66aa97 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-a09e1b4bee"></a>Parser name: `collections`
- <a id="s-c7825680e2"></a>Extra arguments at this parser: rejected.
- <a id="s-f7486a7ebf"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-4d3f08e7a1"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-edb49973f0"></a>`cursor`<br>`--cursor` | required option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-15efc91740"></a>`limit`<br>`--limit` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `100`<br>Env: `null` |
| <a id="s-6d641b0eeb"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5f9af9695c"></a>`help` | <a id="s-5c810ce331"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-fa178adb24"></a>`0` | <a id="s-3c608b965a"></a>`"noncontractual-framework-help"` | <a id="s-8da5cc20fd"></a>`"empty"` |

### Result and failure contract

- <a id="s-a4fc651499"></a>Result identity: `a-riverhog-cli-result/catalog-sync/collections/v1`
- <a id="s-e39a6f5402"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-13eeed53af"></a>Structured output: `optional-json`
- <a id="s-183ffea2d2"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c396a93ad4"></a>`completed` | <a id="s-655b4d9fc8"></a>`{"kind":"command-completed"}` | <a id="s-7ce31655d9"></a>`0` | <a id="s-34329b9ae4"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP list_catalog_sync_collections response 200](../../riverhog/http-operations/get-v1-catalog-sync-collections.md#s-a4ea653adc) | <a id="s-b56347521e"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-16a71c5f8f"></a>`usage` | <a id="s-b256a95b3f"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-18c80f42e2"></a>`2` | <a id="s-2b19990115"></a>all: `"empty"` | <a id="s-1277be8f1c"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-8e83f7f7c5"></a>`operational` | <a id="s-fe6fe3511b"></a>`{"kind":"application-error"}` | <a id="s-80c8329275"></a>`1` | <a id="s-53665b6e3f"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-adb97befdf"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --cursor](#s-edb49973f0) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-6d641b0eeb) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --limit](#s-15efc91740) | `value · cli-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --limit](#s-15efc91740) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/catalog-sync/collections](../../riverhog/http-operations/get-v1-catalog-sync-collections.md)
- [riverhog_client.ApiClient.list_catalog_sync_collections](../../riverhog-client/python/riverhog-client-apiclient-list-catalog-sync-collections.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-905ab27dd8"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-cf8643b46c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::catalog\_sync\_collections\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L789)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/collections/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/collections/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/collections/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/collections/name`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/collections/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/collections/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/collections/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/collections/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/collections/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/collections/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/collections/name`

<!-- exact-contract-value: 4f1fca154c5c9c2f78bcc5156e3a41ce95ddae1c5ffab22801ecc3a0974a38c6 -->

```json
"collections"
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/collections/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/collections/result_contract`

<!-- exact-contract-value: b8f9020eda3d949a8177ffb643b65f89524de7636491df946365949801084c15 -->

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
  "identity": "a-riverhog-cli-result/catalog-sync/collections/v1",
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
          "operation_id": "list_catalog_sync_collections",
          "path": "/v1/catalog-sync/collections",
          "schema": {
            "$ref": "#/components/schemas/CatalogSyncCollectionPage"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/collections/terminating_controls`

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
