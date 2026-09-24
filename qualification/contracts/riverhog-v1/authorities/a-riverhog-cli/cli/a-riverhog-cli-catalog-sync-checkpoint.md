# a-riverhog-cli catalog-sync checkpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-catalog-sync-checkpoint:1577702afb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d4d8999e4c"></a>Parser name: `checkpoint`
- <a id="s-4150507aee"></a>Extra arguments at this parser: rejected.
- <a id="s-5f8a9742c4"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-117803b3da"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-5c708970a4"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c35343b426"></a>`help` | <a id="s-e75f0f2066"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-1158b13c85"></a>`0` | <a id="s-6ec877a177"></a>`"noncontractual-framework-help"` | <a id="s-323fc9183d"></a>`"empty"` |

### Result and failure contract

- <a id="s-fb8f5da8ed"></a>Result identity: `a-riverhog-cli-result/catalog-sync/checkpoint/v1`
- <a id="s-e8f03eb602"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-9e590f98d9"></a>Structured output: `optional-json`
- <a id="s-b5600e97e9"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-39191ead72"></a>`completed` | <a id="s-63bae2e690"></a>`{"kind":"command-completed"}` | <a id="s-d19c861e13"></a>`0` | <a id="s-c28dcfc7ea"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP create_catalog_sync_checkpoint response 200](../../riverhog/http-operations/get-v1-catalog-sync-checkpoint.md#s-d00c372b8d) | <a id="s-678781d63f"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-8d3e40d3d5"></a>`usage` | <a id="s-4d4dbd5a5d"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-40cbbccfb9"></a>`2` | <a id="s-2a133b69c3"></a>all: `"empty"` | <a id="s-663dd7423b"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-94884684f8"></a>`operational` | <a id="s-909ce29a17"></a>`{"kind":"application-error"}` | <a id="s-78bf8cab7e"></a>`1` | <a id="s-f9e4b33730"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-87adc0445a"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-5c708970a4) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/catalog-sync/checkpoint](../../riverhog/http-operations/get-v1-catalog-sync-checkpoint.md)
- [riverhog_client.ApiClient.create_catalog_sync_checkpoint](../../riverhog-client/python/riverhog-client-apiclient-create-catalog-sync-checkpoint.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-06cfea8f6f"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-8d27ccfc5c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::catalog\_sync\_checkpoint\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L772)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/checkpoint/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/checkpoint/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/checkpoint/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/checkpoint/name`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/checkpoint/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/checkpoint/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/checkpoint/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/checkpoint/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/checkpoint/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/checkpoint/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/checkpoint/name`

<!-- exact-contract-value: 59b774a5253e2bc8e357bdfc95e4f7a4b214734f6da65a9f53b40711a10ba16b -->

```json
"checkpoint"
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/checkpoint/parameters`

<!-- exact-contract-value: f2cf9ed04ac608b58219dbcf22fc63be2fdf35901bc058f443df21b229aefd32 -->

```json
[
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

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/checkpoint/result_contract`

<!-- exact-contract-value: 8c1951dc28a33575677ec55803f9cbd11bf73006302deb32d1c482818e8ae81e -->

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
  "identity": "a-riverhog-cli-result/catalog-sync/checkpoint/v1",
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
          "operation_id": "create_catalog_sync_checkpoint",
          "path": "/v1/catalog-sync/checkpoint",
          "schema": {
            "$ref": "#/components/schemas/CatalogSyncCheckpoint"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/commands/checkpoint/terminating_controls`

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
