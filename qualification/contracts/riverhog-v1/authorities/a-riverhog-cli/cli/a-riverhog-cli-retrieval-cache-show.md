# a-riverhog-cli retrieval cache show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-retrieval-cache-show:a7f5f2de91 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-ecb9d812a8"></a>Parser name: `show`
- <a id="s-83a54cc198"></a>Extra arguments at this parser: rejected.
- <a id="s-9ff58c6e72"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-fb19eb9f64"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-307efc1689"></a>`selector`<br>`selector` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-2b1c490c5d"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-59e226b797"></a>`help` | <a id="s-7067515f2b"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-717a3aca83"></a>`0` | <a id="s-a54d504b3f"></a>`"noncontractual-framework-help"` | <a id="s-e2733259e2"></a>`"empty"` |

### Result and failure contract

- <a id="s-54b8ec573c"></a>Result identity: `a-riverhog-cli-result/retrieval/cache/show/v1`
- <a id="s-73d6634cf2"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-7e1dd8105e"></a>Structured output: `optional-json`
- <a id="s-0c10b2b4d4"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6f4a47ae0b"></a>`completed` | <a id="s-d3263a2b70"></a>`{"kind":"command-completed"}` | <a id="s-3f92509096"></a>`0` | <a id="s-34320e5ce5"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_retrieval_cache_object response 200](../../riverhog/http-operations/get-v1-retrieval-cache-objects-collection-id-source-store-object-id.md#s-ffc0a1dc71) | <a id="s-44481178da"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-20486a42ee"></a>`usage` | <a id="s-ec4841de40"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-46b511e455"></a>`2` | <a id="s-cb9f92f5af"></a>all: `"empty"` | <a id="s-6be7dc84a4"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-08f808f69d"></a>`operational` | <a id="s-f100c4d960"></a>`{"kind":"application-error"}` | <a id="s-af4c9209e4"></a>`1` | <a id="s-adb6964f0e"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-29f0359e71"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-2b1c490c5d) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter selector](#s-307efc1689) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/retrieval-cache/objects/{collection_id}/{source_store}/{object_id}](../../riverhog/http-operations/get-v1-retrieval-cache-objects-collection-id-source-store-object-id.md)
- [riverhog_client.ApiClient.get_retrieval_cache_object](../../riverhog-client/python/riverhog-client-apiclient-get-retrieval-cache-object.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-6564a716ea"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-b0214c7f16"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::retrieval\_cache\_show\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2889)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/show/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/show/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/show/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/show/name`
- `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/show/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/show/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/show/parameters`

<!-- exact-contract-value: 6b32bc56c11c2cdafb747d7a7aa740468c2f614b5b1d0e2588fb031d746e52f9 -->

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

### `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/show/result_contract`

<!-- exact-contract-value: c579a4f4c205a89fbad99dd33bd43f116216c38b80071ad2c7df43f7014f2b5a -->

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
  "identity": "a-riverhog-cli-result/retrieval/cache/show/v1",
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
          "operation_id": "get_retrieval_cache_object",
          "path": "/v1/retrieval-cache/objects/{collection_id}/{source_store}/{object_id}",
          "schema": {
            "$ref": "#/components/schemas/RetrievalCacheObjectOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/retrieval/commands/cache/commands/show/terminating_controls`

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
