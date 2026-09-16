# piggity archive store show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-archive-store-show:a478a8de55 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-3bb7cf628c"></a>Parser name: `show`
- <a id="s-8a388be16b"></a>Extra arguments at this parser: rejected.
- <a id="s-d990eca5fb"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-e4b628d48a"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-ca052d74b2"></a>`store`<br>`store` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-dc26e9779c"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-867d1e7a25"></a>`help` | <a id="s-900ba5e7ab"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-2c81bb8645"></a>`0` | <a id="s-c4e471a6a5"></a>`"noncontractual-framework-help"` | <a id="s-4e3a41461f"></a>`"empty"` |

### Result and failure contract

- <a id="s-e6e37cff5a"></a>Result identity: `piggity-cli-result/archive/store/show/v1`
- <a id="s-31c51f3488"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-ac6251e451"></a>Structured output: `optional-json`
- <a id="s-8400005ee9"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-346a2d783e"></a>`completed` | <a id="s-4cb80675f6"></a>`{"kind":"command-completed"}` | <a id="s-d3b9442edc"></a>`0` | <a id="s-7090a866e2"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_archive_store response 200](../../riverhog/http-operations/get-v1-archive-stores-store.md#s-0c14d5abd4) | <a id="s-3e8c10bb8d"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f0b6769b0d"></a>`usage` | <a id="s-49fff30e5e"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-7d12c510b1"></a>`2` | <a id="s-e2bc3309a2"></a>all: `"empty"` | <a id="s-13d528ea71"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-a491f1557a"></a>`operational` | <a id="s-c7d8d17d47"></a>`{"kind":"application-error"}` | <a id="s-72e8b34232"></a>`1` | <a id="s-2040db8bb8"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-3b43216784"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-dc26e9779c) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter store](#s-ca052d74b2) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/archive/stores/{store}](../../riverhog/http-operations/get-v1-archive-stores-store.md)
- [riverhog_client.ApiClient.get_archive_store](../../riverhog-client/python/riverhog-client-apiclient-get-archive-store.md)

## Governing policies

- <a id="pa-89803a94ac"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-ae0457aa9f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::archive_store_show_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2900)

### Machine authority

- `/external_contract/cli/piggity/commands/archive/commands/store/commands/show/allow_extra_args`
- `/external_contract/cli/piggity/commands/archive/commands/store/commands/show/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/archive/commands/store/commands/show/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/archive/commands/store/commands/show/name`
- `/external_contract/cli/piggity/commands/archive/commands/store/commands/show/parameters`
- `/external_contract/cli/piggity/commands/archive/commands/store/commands/show/result_contract`
- `/external_contract/cli/piggity/commands/archive/commands/store/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/archive/commands/store/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/archive/commands/store/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/archive/commands/store/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/archive/commands/store/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/piggity/commands/archive/commands/store/commands/show/parameters`

<!-- exact-contract-value: 2ab61ec9c192c9fe10fe540b4a5168a5e5a2955bd2e7d2a0b778a648527896b2 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "store",
    "nargs": 1,
    "options": [
      "store"
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

### `/external_contract/cli/piggity/commands/archive/commands/store/commands/show/result_contract`

<!-- exact-contract-value: ac5279b0261184448496b2a43d7a939385bee2d9cf30e859f6f6091133e46b54 -->

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
  "identity": "piggity-cli-result/archive/store/show/v1",
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
          "operation_id": "get_archive_store",
          "path": "/v1/archive/stores/{store}",
          "schema": {
            "$ref": "#/components/schemas/ArchiveStoreOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/archive/commands/store/commands/show/terminating_controls`

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
