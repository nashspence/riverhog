# gogurt provider mounted-volume list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-provider-mounted-volume-list:0fbd3640ba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-b69581520f"></a>Parser name: `list`
- <a id="s-c01192f0e1"></a>Extra arguments at this parser: rejected.
- <a id="s-97c233ce4b"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-e0deae5ba3"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-39f1dc9eb2"></a>`ids`<br>`--ids` | optional flag; 0 values | boolean | `false`<br>Env: `null` |
| <a id="s-20a1505b65"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-915aa1f759"></a>`help` | <a id="s-18c1d33322"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-2cc5a4825e"></a>`0` | <a id="s-16ea523617"></a>`"noncontractual-framework-help"` | <a id="s-8c3df9bb4f"></a>`"empty"` |

### Result and failure contract

- <a id="s-cd531fdc6d"></a>Result identity: `gogurt-cli-result/provider/mounted-volume/list/v1`
- <a id="s-46a8079d5a"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-1583142758"></a>Structured output: `optional-json`
- <a id="s-3f4c055c08"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-cf8f2c4504"></a>`completed` | <a id="s-50f315949d"></a>`{"kind":"command-completed"}` | <a id="s-cc1b9f9be2"></a>`0` | <a id="s-6425bca83c"></a>human: `"noncontractual-presentation-of-command-result"`; json: [gogurt-provider-list/v1](#s-6b2cf5a211) | <a id="s-536936b94e"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-32cb5a3c52"></a>`usage` | <a id="s-b6eae10f8d"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-8115df8409"></a>`2` | <a id="s-a944f4f084"></a>all: `"empty"` | <a id="s-bebd7a001c"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-ccfca921f6"></a>`operational` | <a id="s-72e98de6a5"></a>`{"kind":"application-error"}` | <a id="s-ec8d487fe3"></a>`1` | <a id="s-1c58824587"></a>human: `"empty"`; json: [gogurt-cli-error/v1](#s-981389e8d3) | <a id="s-71a4576897"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-6b2cf5a211"></a>`gogurt-provider-list/v1`

Applies to: completed · stdout (json).

<a id="s-6faa326985"></a>

- <a id="s-e270606e43"></a>`type`: `"object"`
- <a id="s-312d1201cc"></a>`additionalProperties`: `false`
- <a id="s-1a8dfb67d5"></a>`required`: `["format","kind","providers"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8da9da63e5"></a>`format` | yes | const="gogurt-provider-list/v1" |  |
| <a id="s-9383f25836"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| `providers` | yes | [See field `providers`](#s-a726d05010) |  |

##### <a id="s-a726d05010"></a>field `providers`

- <a id="s-e351e3f84c"></a>`type`: `"array"`
- `items`: [See field `providers` · `items`](#s-ba58089c1d)

##### <a id="s-ba58089c1d"></a>field `providers` · `items`

- <a id="s-4c783e1592"></a>`type`: `"object"`
- <a id="s-0638157eab"></a>`additionalProperties`: `false`
- <a id="s-5d24c5feba"></a>`required`: `["kind","name","entry_point","distribution","version"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5f3a8ddce1"></a>`distribution` | yes | type=["string","null"] |  |
| <a id="s-30080be83f"></a>`entry_point` | yes | type="string" |  |
| <a id="s-59f36972da"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-8aa24e9884"></a>`name` | yes | type="string" |  |
| <a id="s-9612560306"></a>`version` | yes | type=["string","null"] |  |

#### <a id="s-981389e8d3"></a>`gogurt-cli-error/v1`

Applies to: operational · stdout (json).

<a id="s-ca093d35b6"></a>

- <a id="s-c3675f34ab"></a>`type`: `"object"`
- <a id="s-55c5995b9d"></a>`additionalProperties`: `false`
- <a id="s-75933fabbd"></a>`required`: `["error"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | [See field `error`](#s-6918202cad) |  |

##### <a id="s-6918202cad"></a>field `error`

- <a id="s-cd07db5248"></a>`type`: `"object"`
- <a id="s-aa876580dd"></a>`additionalProperties`: `false`
- <a id="s-eb57a71df7"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a58434865"></a>`code` | yes | enum=["config_error","listener_error"] |  |
| <a id="s-d0923a834b"></a>`message` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --ids](#s-39f1dc9eb2) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-20a1505b65) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-ba1d0098e3"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-1194a5526e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — [reference/gogurt/application/src/gogurt/cli.py::&lt;module&gt;](../../../../../../reference/gogurt/application/src/gogurt/cli.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/allow_extra_args`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/name`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/parameters`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/result_contract`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/parameters`

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

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/result_contract`

<!-- exact-contract-value: 7272927dd7761ba4ac6fab540d168dbbdd36483168d6a14876bb672e92f01d4c -->

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
  "identity": "gogurt-cli-result/provider/mounted-volume/list/v1",
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

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/terminating_controls`

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
