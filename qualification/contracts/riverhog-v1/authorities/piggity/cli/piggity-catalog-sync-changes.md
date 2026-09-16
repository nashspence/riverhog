# piggity catalog-sync changes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-catalog-sync-changes:c21f25f653 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-38d6d14f53"></a>Parser name: `changes`
- <a id="s-06495d28b6"></a>Extra arguments at this parser: rejected.
- <a id="s-090b3c8a4a"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-847599a635"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-b81c26e798"></a>`cursor`<br>`--cursor` | required option; 1 value | text | not recorded |
| <a id="s-5596b60bf3"></a>`limit`<br>`--limit` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `100` |
| <a id="s-ad19e2621a"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5096151b53"></a>`help` | <a id="s-097d081fd4"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-2316581323"></a>`0` | <a id="s-161d540501"></a>`"noncontractual-framework-help"` | <a id="s-e96f1aed55"></a>`"empty"` |

### Result and failure contract

- <a id="s-874943888f"></a>Result identity: `piggity-cli-result/catalog-sync/changes/v1`
- <a id="s-3174d9c222"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-ece2972a17"></a>Structured output: `optional-json`
- <a id="s-54deab4725"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b7a8ccf908"></a>`completed` | <a id="s-30a8afd188"></a>`{"kind":"command-completed"}` | <a id="s-5c1f1c1eed"></a>`0` | <a id="s-83632840d4"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP list_catalog_sync_changes response 200](../../riverhog/http-operations/get-v1-catalog-sync-changes.md#s-965d91e8aa) | <a id="s-8bf5b32ce4"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-84774d6f48"></a>`usage` | <a id="s-77ca0270d4"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-7d2289a7c5"></a>`2` | <a id="s-a464a64ce1"></a>all: `empty` | <a id="s-b6934519cf"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-f07818d961"></a>`operational` | <a id="s-07af3097d8"></a>`{"kind":"application-error"}` | <a id="s-91e56464ad"></a>`1` | <a id="s-450409e802"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-890030578d"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --cursor](#s-b81c26e798) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-ad19e2621a) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"} |
| [CLI parameter --limit](#s-5596b60bf3) | `value · cli-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --limit](#s-5596b60bf3) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/catalog-sync/changes](../../riverhog/http-operations/get-v1-catalog-sync-changes.md)
- [riverhog_client.ApiClient.list_catalog_sync_changes](../../riverhog-client/python/riverhog-client-apiclient-list-catalog-sync-changes.md)

## Governing policies

- <a id="pa-23408f750a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-096ed91a7d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::catalog_sync_changes_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L813)

### Machine authority

- `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/allow_extra_args`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/name`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/parameters`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/result_contract`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/name`

<!-- exact-contract-value: 1914b3c81072ec685b513a4497b8db642b0328afb8aeaede6dd78a6311879dbe -->

```json
"changes"
```

### `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/parameters`

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

### `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/result_contract`

<!-- exact-contract-value: 88d367d034da730fa62c67991e1f1d6194842754ee7369a33efb9610f974bbd8 -->

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
  "identity": "piggity-cli-result/catalog-sync/changes/v1",
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

### `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/terminating_controls`

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
