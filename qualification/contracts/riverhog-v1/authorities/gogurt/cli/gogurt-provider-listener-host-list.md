# gogurt provider listener-host list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-provider-listener-host-list:8bddc46d92 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-ac80695136"></a>Parser name: `list`
- <a id="s-82e876201e"></a>Extra arguments at this parser: rejected.
- <a id="s-ce70dd4dc7"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-3f9090bfbe"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-da38580369"></a>`ids`<br>`--ids` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-7428604f93"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-0239e2c562"></a>`help` | <a id="s-eed54660ba"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-59aa202811"></a>`0` | <a id="s-4419a71184"></a>`"noncontractual-framework-help"` | <a id="s-aa1e1a071d"></a>`"empty"` |

### Result and failure contract

- <a id="s-9c1c9be4d0"></a>Result identity: `gogurt-cli-result/provider/listener-host/list/v1`
- <a id="s-20c7af8591"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-db4a6b0e64"></a>Structured output: `optional-json`
- <a id="s-31f9f2a643"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b7fcf63dc6"></a>`completed` | <a id="s-f6d51d477d"></a>`{"kind":"command-completed"}` | <a id="s-8258eab3ff"></a>`0` | <a id="s-f8c32bd2f5"></a>human: `"noncontractual-presentation-of-command-result"`; json: [gogurt-provider-list/v1](#s-526173c7b9) | <a id="s-8fd7527f5a"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-afc40ae64b"></a>`usage` | <a id="s-6a13de0f39"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-2bafaab693"></a>`2` | <a id="s-05cfa13255"></a>all: `"empty"` | <a id="s-afa5cce429"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-4e487506f4"></a>`operational` | <a id="s-e6db970086"></a>`{"kind":"application-error"}` | <a id="s-126c99792b"></a>`1` | <a id="s-3ca0f36b10"></a>human: `"empty"`; json: [gogurt-cli-error/v1](#s-49807a8132) | <a id="s-134483f0ec"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-526173c7b9"></a>`gogurt-provider-list/v1`

Applies to: completed · stdout (json).

<a id="s-668f4cc9ca"></a>

- <a id="s-2f6d7e0c2e"></a>`type`: `"object"`
- <a id="s-dc5be72b33"></a>`additionalProperties`: `false`
- <a id="s-9136f95782"></a>`required`: `["format","kind","providers"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a272bccb7d"></a>`format` | yes | const="gogurt-provider-list/v1" |  |
| <a id="s-10927e48b7"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| `providers` | yes | [See field `providers`](#s-6f5d2c195a) |  |

##### <a id="s-6f5d2c195a"></a>field `providers`

- <a id="s-41f2526507"></a>`type`: `"array"`
- `items`: [See field `providers` · `items`](#s-7bbe6058de)

##### <a id="s-7bbe6058de"></a>field `providers` · `items`

- <a id="s-6eb3bd9b8f"></a>`type`: `"object"`
- <a id="s-d9d52bb8b8"></a>`additionalProperties`: `false`
- <a id="s-823b96a58c"></a>`required`: `["kind","name","entry_point","distribution","version"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6a274922bf"></a>`distribution` | yes | type=["string","null"] |  |
| <a id="s-b5595e05cc"></a>`entry_point` | yes | type="string" |  |
| <a id="s-f273669502"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-2d42d04167"></a>`name` | yes | type="string" |  |
| <a id="s-784af72835"></a>`version` | yes | type=["string","null"] |  |

#### <a id="s-49807a8132"></a>`gogurt-cli-error/v1`

Applies to: operational · stdout (json).

<a id="s-8c8497ace8"></a>

- <a id="s-d82337f907"></a>`type`: `"object"`
- <a id="s-aba5de5c33"></a>`additionalProperties`: `false`
- <a id="s-473517a67e"></a>`required`: `["error"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | [See field `error`](#s-fb4cdce2de) |  |

##### <a id="s-fb4cdce2de"></a>field `error`

- <a id="s-bf4f546828"></a>`type`: `"object"`
- <a id="s-d66f9ae8c9"></a>`additionalProperties`: `false`
- <a id="s-86c0383d6c"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3a14137643"></a>`code` | yes | enum=["config_error","listener_error"] |  |
| <a id="s-5620ae6c37"></a>`message` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --ids](#s-da38580369) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-7428604f93) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-9b55b1caab"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-26485e325a"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources/authorities.md#src-3b2297c37d) — [reference/gogurt/application/src/gogurt/cli.py::&lt;module&gt;](../../../../../../reference/gogurt/application/src/gogurt/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/allow_extra_args`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/name`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/parameters`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/result_contract`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/parameters`

<!-- exact-contract-value: e1d69a72acbf63fdc3459d233dcdf028761244a8bf1785d2a24bd76449d6da79 -->

```json
[
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "ids",
    "nargs": 1,
    "options": [
      "--ids"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
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

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/result_contract`

<!-- exact-contract-value: 0013de87cd8ce9f11a9002d267438c36f01bb189bedfbe266e307745cf15cdec -->

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
          "identity": "gogurt-cli-error/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "error": {
                "additionalProperties": false,
                "properties": {
                  "code": {
                    "enum": [
                      "config_error",
                      "listener_error"
                    ]
                  },
                  "message": {
                    "type": "string"
                  }
                },
                "required": [
                  "code",
                  "message"
                ],
                "type": "object"
              }
            },
            "required": [
              "error"
            ],
            "type": "object"
          }
        }
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "gogurt-cli-result/provider/listener-host/list/v1",
  "profile_id": "gogurt-cli-human-json/v1",
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
          "identity": "gogurt-provider-list/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "format": {
                "const": "gogurt-provider-list/v1"
              },
              "kind": {
                "enum": [
                  "mounted-volume",
                  "listener-host"
                ]
              },
              "providers": {
                "items": {
                  "additionalProperties": false,
                  "properties": {
                    "distribution": {
                      "type": [
                        "string",
                        "null"
                      ]
                    },
                    "entry_point": {
                      "type": "string"
                    },
                    "kind": {
                      "enum": [
                        "mounted-volume",
                        "listener-host"
                      ]
                    },
                    "name": {
                      "type": "string"
                    },
                    "version": {
                      "type": [
                        "string",
                        "null"
                      ]
                    }
                  },
                  "required": [
                    "kind",
                    "name",
                    "entry_point",
                    "distribution",
                    "version"
                  ],
                  "type": "object"
                },
                "type": "array"
              }
            },
            "required": [
              "format",
              "kind",
              "providers"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/list/terminating_controls`

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
