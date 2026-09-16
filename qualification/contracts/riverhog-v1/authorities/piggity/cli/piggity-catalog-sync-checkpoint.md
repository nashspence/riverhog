# piggity catalog-sync checkpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-catalog-sync-checkpoint:f9fab3e487 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d3fa47b142"></a>Parser name: `checkpoint`
- <a id="s-573917d2f5"></a>Extra arguments at this parser: rejected.
- <a id="s-1f7330d718"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-9a04855c85"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-dcf2b49b77"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5fcbc91f97"></a>`help` | <a id="s-70c8d92349"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c40ff92fc2"></a>`0` | <a id="s-d015d3b5a9"></a>`"noncontractual-framework-help"` | <a id="s-2f5668de12"></a>`"empty"` |

### Result and failure contract

- <a id="s-2b0b2b2d1f"></a>Result identity: `piggity-cli-result/catalog-sync/checkpoint/v1`
- <a id="s-6862e73e69"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-caf119ba46"></a>Structured output: `optional-json`
- <a id="s-6cb892b83a"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0b2d7883da"></a>`completed` | <a id="s-60ffb0e7e3"></a>`{"kind":"command-completed"}` | <a id="s-252be26c00"></a>`0` | <a id="s-ebe7483078"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP create_catalog_sync_checkpoint response 200](../../riverhog/http-operations/get-v1-catalog-sync-checkpoint.md#s-d00c372b8d) | <a id="s-47c63667db"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-7681051a14"></a>`usage` | <a id="s-8ecb9fdd88"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-21afa4537a"></a>`2` | <a id="s-13641a298c"></a>all: `"empty"` | <a id="s-e1af3e495e"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-a88d115143"></a>`operational` | <a id="s-ec61f0b457"></a>`{"kind":"application-error"}` | <a id="s-9d05105219"></a>`1` | <a id="s-82f0f7edf1"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-afe412afa3"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-dcf2b49b77) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/catalog-sync/checkpoint](../../riverhog/http-operations/get-v1-catalog-sync-checkpoint.md)
- [riverhog_client.ApiClient.create_catalog_sync_checkpoint](../../riverhog-client/python/riverhog-client-apiclient-create-catalog-sync-checkpoint.md)

## Governing policies

- <a id="pa-5191603172"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-7cd0cec78f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::catalog_sync_checkpoint_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L770)

### Machine authority

- `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/allow_extra_args`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/name`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/parameters`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/result_contract`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/name`

<!-- exact-contract-value: 59b774a5253e2bc8e357bdfc95e4f7a4b214734f6da65a9f53b40711a10ba16b -->

```json
"checkpoint"
```

### `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/parameters`

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

### `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/result_contract`

<!-- exact-contract-value: c430d75270f51d412dd5bb215ee892f270289f17bb0a7a3b2c4dd0e01b0f7357 -->

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
  "identity": "piggity-cli-result/catalog-sync/checkpoint/v1",
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

### `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/terminating_controls`

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
