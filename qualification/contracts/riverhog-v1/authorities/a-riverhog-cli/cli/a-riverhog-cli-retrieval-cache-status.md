# a-riverhog-cli retrieval cache status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-retrieval-cache-status:6af09c8ce0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-ea2dd80d8a"></a>Parser name: `status`
- <a id="s-37c294e804"></a>Extra arguments at this parser: rejected.
- <a id="s-9e4e5362ad"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-d959aa78d3"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-cfaed11650"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-f021e4bc00"></a>`help` | <a id="s-eacc51ef5e"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-dda29d3e63"></a>`0` | <a id="s-b53d3403f9"></a>`"noncontractual-framework-help"` | <a id="s-0b95ae4b87"></a>`"empty"` |

### Result and failure contract

- <a id="s-c9864539fd"></a>Result identity: `a-riverhog-cli-result/retrieval/cache/status/v1`
- <a id="s-7c38d04639"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-b613a3c8e0"></a>Structured output: `optional-json`
- <a id="s-f4df333263"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-96471498ce"></a>`completed` | <a id="s-13dbe0d75d"></a>`{"kind":"command-completed"}` | <a id="s-75c1c9b18b"></a>`0` | <a id="s-dd8e8e4059"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP retrieval_cache_status response 200](../../riverhog/http-operations/get-v1-retrieval-cache.md#s-27e211ffbb) | <a id="s-29043f63d2"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-dc98af972c"></a>`usage` | <a id="s-391a02c629"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-2c187d14b0"></a>`2` | <a id="s-6b1c8309c9"></a>all: `"empty"` | <a id="s-5b8dfd39bf"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-1b09bc76cf"></a>`operational` | <a id="s-bf78c6628b"></a>`{"kind":"application-error"}` | <a id="s-6e0741755f"></a>`1` | <a id="s-216d46f7fc"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-3435f0cd80"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-cfaed11650) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/retrieval-cache](../../riverhog/http-operations/get-v1-retrieval-cache.md)
- [riverhog_client.ApiClient.retrieval_cache_status](../../riverhog-client/python/riverhog-client-apiclient-retrieval-cache-status.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-ae2c7db7bc"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-35d30e5d73"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::retrieval\_cache\_status\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2782)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/status/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/status/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/status/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/status/name`
- `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/status/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/status/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/status/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/status/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/status/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/status/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/status/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/status/result_contract`

<!-- exact-contract-value: 26e43b1505cb0241d0e643c664a3b583f7ea99c22f2ccf3f7fafb101f4442dce -->

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
  "identity": "a-riverhog-cli-result/retrieval/cache/status/v1",
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

### `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/status/terminating_controls`

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
