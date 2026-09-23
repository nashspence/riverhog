# gogurt provider mounted-volume show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-provider-mounted-volume-show:a4ee176db7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-85eac40052"></a>Parser name: `show`
- <a id="s-e9a7313ae4"></a>Extra arguments at this parser: rejected.
- <a id="s-25a23692d2"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-3f6353d9e5"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-ae67412ef1"></a>`name`<br>`name` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-acb58db353"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-15802b4e32"></a>`help` | <a id="s-83559fb3b5"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c62e10e314"></a>`0` | <a id="s-f85040ea48"></a>`"noncontractual-framework-help"` | <a id="s-38e718f845"></a>`"empty"` |

### Result and failure contract

- <a id="s-35b6418c64"></a>Result identity: `gogurt-cli-result/provider/mounted-volume/show/v1`
- <a id="s-156414f2b5"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-00ed1397c1"></a>Structured output: `optional-json`
- <a id="s-903599bc43"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-82531474d0"></a>`completed` | <a id="s-9c7fffa628"></a>`{"kind":"command-completed"}` | <a id="s-eebd7952ce"></a>`0` | <a id="s-e781cfdf07"></a>human: `"noncontractual-presentation-of-command-result"`; json: [gogurt-provider-detail/v1](#s-b99f2f7507) | <a id="s-1d22d8a484"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-4597c7afef"></a>`usage` | <a id="s-74bc45ae39"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-4c76ac66dd"></a>`2` | <a id="s-f5c11ddb2a"></a>all: `"empty"` | <a id="s-cdaaf801bc"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-2a6fa2398a"></a>`operational` | <a id="s-9dd8209a13"></a>`{"kind":"application-error"}` | <a id="s-6720ff73ce"></a>`1` | <a id="s-ecad4578e9"></a>human: `"empty"`; json: [gogurt-cli-error/v1](#s-8b5b654790) | <a id="s-0c70c02b19"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-b99f2f7507"></a>`gogurt-provider-detail/v1`

Applies to: completed · stdout (json).

<a id="s-8fb0f66282"></a>

- <a id="s-ac77098f90"></a>`type`: `"object"`
- <a id="s-22cae1e26d"></a>`additionalProperties`: `false`
- <a id="s-56e27071e2"></a>`required`: `["kind","name","entry_point","distribution","version","reference"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-078fdcf525"></a>`distribution` | yes | type=["string","null"] |  |
| <a id="s-0a2e370e69"></a>`entry_point` | yes | type="string" |  |
| <a id="s-32ddecbb76"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-4c30afafaa"></a>`name` | yes | type="string" |  |
| `reference` | yes | [See field `reference`](#s-21a0d1ea27) |  |
| <a id="s-86d6bc85d4"></a>`version` | yes | type=["string","null"] |  |

##### <a id="s-21a0d1ea27"></a>field `reference`

- <a id="s-badd497c6a"></a>`type`: `"object"`
- <a id="s-ff82a549bd"></a>`additionalProperties`: `false`
- <a id="s-9a98eb045b"></a>`required`: `["kind","name","provider_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e18017298f"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-c70f0b352d"></a>`name` | yes | type="string" |  |
| <a id="s-26e4f61152"></a>`provider_id` | yes | type="string" |  |

#### <a id="s-8b5b654790"></a>`gogurt-cli-error/v1`

Applies to: operational · stdout (json).

<a id="s-55aa1b0763"></a>

- <a id="s-92ace5038b"></a>`type`: `"object"`
- <a id="s-0d5fea07bf"></a>`additionalProperties`: `false`
- <a id="s-0010d58d56"></a>`required`: `["error"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | [See field `error`](#s-8c27146c1f) |  |

##### <a id="s-8c27146c1f"></a>field `error`

- <a id="s-c1f319a7f0"></a>`type`: `"object"`
- <a id="s-e34eeeacc7"></a>`additionalProperties`: `false`
- <a id="s-ce5e80d305"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c21dae4767"></a>`code` | yes | enum=["config_error","listener_error"] |  |
| <a id="s-142db51523"></a>`message` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-acb58db353) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter name](#s-ae67412ef1) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-062d4a3408"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-378d69a325"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources/authorities.md#src-3b2297c37d) — [some-implementations/gogurt/application/src/gogurt/cli.py::&lt;module&gt;](../../../../../../some-implementations/gogurt/application/src/gogurt/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/allow_extra_args`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/name`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/parameters`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/result_contract`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/parameters`

<!-- exact-contract-value: 5fc7dfae38d65070ca7b7f9d59934174307d6a3212e66d1af82fe71c64370451 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "name",
    "nargs": 1,
    "options": [
      "name"
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

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/result_contract`

<!-- exact-contract-value: 4417431fcf8ca51492efc73264cd9a2d3fd4c11c26fed66635c77edcd2d6c253 -->

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
  "identity": "gogurt-cli-result/provider/mounted-volume/show/v1",
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
          "identity": "gogurt-provider-detail/v1",
          "kind": "cli-local-json-schema",
          "schema": {
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
              "reference": {
                "additionalProperties": false,
                "properties": {
                  "kind": {
                    "enum": [
                      "mounted-volume",
                      "listener-host"
                    ]
                  },
                  "name": {
                    "type": "string"
                  },
                  "provider_id": {
                    "type": "string"
                  }
                },
                "required": [
                  "kind",
                  "name",
                  "provider_id"
                ],
                "type": "object"
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
              "version",
              "reference"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/terminating_controls`

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
