# piggity retrieval cache status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-retrieval-cache-status:be91c34c7f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-cd2252804c"></a>Parser name: `status`
- <a id="s-39b521340a"></a>Extra arguments at this parser: rejected.
- <a id="s-6347419c6e"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-25532ecb3d"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-fffd71beac"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-faec9ff9a5"></a>`help` | <a id="s-5cef29d24b"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-53e6186cf6"></a>`0` | <a id="s-7c35b14d35"></a>`"noncontractual-framework-help"` | <a id="s-3aadee8ae7"></a>`"empty"` |

### Result and failure contract

- <a id="s-e7560570e4"></a>Result identity: `piggity-cli-result/retrieval/cache/status/v1`
- <a id="s-574f281e70"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-b0c71591d2"></a>Structured output: `optional-json`
- <a id="s-e3b803a9fe"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-13d2cdccbe"></a>`completed` | <a id="s-f8e0ec8033"></a>`{"kind":"command-completed"}` | <a id="s-1754f2a616"></a>`0` | <a id="s-65d6b0257b"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP retrieval_cache_status response 200](../../riverhog/http-operations/get-v1-retrieval-cache.md#s-27e211ffbb) | <a id="s-a34a7a1724"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-486ce53318"></a>`usage` | <a id="s-fb02952a56"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-3db902f788"></a>`2` | <a id="s-6b588973f2"></a>all: `empty` | <a id="s-842f344a57"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-8cb7a7e021"></a>`operational` | <a id="s-52ecd27560"></a>`{"kind":"application-error"}` | <a id="s-5db1d65837"></a>`1` | <a id="s-9b115e39c8"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-0d6acdc611"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-fffd71beac) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/retrieval-cache](../../riverhog/http-operations/get-v1-retrieval-cache.md)
- [riverhog_client.ApiClient.retrieval_cache_status](../../riverhog-client/python/riverhog-client-apiclient-retrieval-cache-status.md)

## Governing policies

- <a id="pa-6249452792"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-97a859afac"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::retrieval_cache_status_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L2767)

### Machine authority

- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/allow_extra_args`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/name`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/parameters`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/result_contract`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/parameters`

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

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/result_contract`

<!-- exact-contract-value: ca89195b835b0c5e48428f5c42bbf88149e1497f44d714f8ad3174d4c1aede14 -->

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
  "identity": "piggity-cli-result/retrieval/cache/status/v1",
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
          "operation_id": "retrieval_cache_status",
          "path": "/v1/retrieval-cache",
          "schema": {
            "$ref": "#/components/schemas/RetrievalCacheStatusOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/terminating_controls`

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
