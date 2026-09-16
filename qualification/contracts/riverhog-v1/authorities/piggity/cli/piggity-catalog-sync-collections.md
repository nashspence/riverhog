# piggity catalog-sync collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-catalog-sync-collections:cba2f84b9b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-25ed634fd5"></a>Parser name: `collections`
- <a id="s-dfdb288cab"></a>Extra arguments at this parser: rejected.
- <a id="s-873c6d1cc3"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-e77798b55b"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-40f6bd513a"></a>`cursor`<br>`--cursor` | required option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-f68f4e4a68"></a>`limit`<br>`--limit` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `100`<br>Env: `null` |
| <a id="s-8905bd1240"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-6f8b29cf25"></a>`help` | <a id="s-e0cc17960b"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-b1d248ab27"></a>`0` | <a id="s-26f7a100a1"></a>`"noncontractual-framework-help"` | <a id="s-1bd4222cc2"></a>`"empty"` |

### Result and failure contract

- <a id="s-2c6dc3d18c"></a>Result identity: `piggity-cli-result/catalog-sync/collections/v1`
- <a id="s-f9136e493c"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-d9f28ae16a"></a>Structured output: `optional-json`
- <a id="s-080dca2e87"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-311cb7f837"></a>`completed` | <a id="s-bd28835234"></a>`{"kind":"command-completed"}` | <a id="s-c418c7b8e9"></a>`0` | <a id="s-61d09548e8"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP list_catalog_sync_collections response 200](../../riverhog/http-operations/get-v1-catalog-sync-collections.md#s-a4ea653adc) | <a id="s-06a1cf6557"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-64e6466e02"></a>`usage` | <a id="s-066fdb1238"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-cefa49693a"></a>`2` | <a id="s-298954244d"></a>all: `"empty"` | <a id="s-e5520e662d"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-55a5a124e3"></a>`operational` | <a id="s-2d6b38bce1"></a>`{"kind":"application-error"}` | <a id="s-470319a487"></a>`1` | <a id="s-a5925cf906"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-b37d34d1de"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --cursor](#s-40f6bd513a) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-8905bd1240) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --limit](#s-f68f4e4a68) | `value · cli-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --limit](#s-f68f4e4a68) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/catalog-sync/collections](../../riverhog/http-operations/get-v1-catalog-sync-collections.md)
- [riverhog_client.ApiClient.list_catalog_sync_collections](../../riverhog-client/python/riverhog-client-apiclient-list-catalog-sync-collections.md)

## Governing policies

- <a id="pa-7eeb011cfa"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-b032895811"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::catalog_sync_collections_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L787)

### Machine authority

- `/external_contract/cli/piggity/commands/catalog-sync/commands/collections/allow_extra_args`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/collections/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/collections/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/collections/name`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/collections/parameters`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/collections/result_contract`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/collections/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/catalog-sync/commands/collections/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/catalog-sync/commands/collections/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/catalog-sync/commands/collections/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/catalog-sync/commands/collections/name`

<!-- exact-contract-value: 4f1fca154c5c9c2f78bcc5156e3a41ce95ddae1c5ffab22801ecc3a0974a38c6 -->

```json
"collections"
```

### `/external_contract/cli/piggity/commands/catalog-sync/commands/collections/parameters`

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

### `/external_contract/cli/piggity/commands/catalog-sync/commands/collections/result_contract`

<!-- exact-contract-value: 9505084062e3c7340ddea7d4969e0eae41f194cf3b5799dbbb19e9619aaaa63f -->

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
  "identity": "piggity-cli-result/catalog-sync/collections/v1",
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

### `/external_contract/cli/piggity/commands/catalog-sync/commands/collections/terminating_controls`

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
