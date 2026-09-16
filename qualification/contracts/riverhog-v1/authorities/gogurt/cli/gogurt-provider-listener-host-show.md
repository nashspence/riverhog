# gogurt provider listener-host show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-provider-listener-host-show:4f43d6637c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-76a22ed842"></a>Parser name: `show`
- <a id="s-a43ca7d9c3"></a>Extra arguments at this parser: rejected.
- <a id="s-f9df34f430"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-07a9c2b38e"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-43595aca53"></a>`name`<br>`name` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-6562a89368"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-d11d96c4a3"></a>`help` | <a id="s-0e6a2065cf"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-aa397d0ce1"></a>`0` | <a id="s-968d9ddcdd"></a>`"noncontractual-framework-help"` | <a id="s-73c196dc9c"></a>`"empty"` |

### Result and failure contract

- <a id="s-9e46d1040c"></a>Result identity: `gogurt-cli-result/provider/listener-host/show/v1`
- <a id="s-bce46d1005"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-8147a7e9a4"></a>Structured output: `optional-json`
- <a id="s-2b2790f28b"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f2872ce970"></a>`completed` | <a id="s-2ef62c44ec"></a>`{"kind":"command-completed"}` | <a id="s-1fea2cdeba"></a>`0` | <a id="s-3897178395"></a>human: `"noncontractual-presentation-of-command-result"`; json: [gogurt-provider-detail/v1](#s-668531c8e9) | <a id="s-d44848cecf"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1ce6efb890"></a>`usage` | <a id="s-401771ee2a"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-f7849bc34d"></a>`2` | <a id="s-dd09d8de7e"></a>all: `"empty"` | <a id="s-59d7e31cc8"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-25f6db944e"></a>`operational` | <a id="s-104e232035"></a>`{"kind":"application-error"}` | <a id="s-ecf9b19181"></a>`1` | <a id="s-25a818fa0f"></a>human: `"empty"`; json: [gogurt-cli-error/v1](#s-e5866811c2) | <a id="s-3fe7e00c93"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-668531c8e9"></a>`gogurt-provider-detail/v1`

Applies to: completed · stdout (json).

<a id="s-5e6c7c58ab"></a>

- <a id="s-29fcdcf4c4"></a>`type`: `"object"`
- <a id="s-39be1c1363"></a>`additionalProperties`: `false`
- <a id="s-67aed246d7"></a>`required`: `["kind","name","entry_point","distribution","version","reference"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f4d7b74f9a"></a>`distribution` | yes | type=["string","null"] |  |
| <a id="s-0cc0e613a0"></a>`entry_point` | yes | type="string" |  |
| <a id="s-2e70b9679b"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-eecc17c5ee"></a>`name` | yes | type="string" |  |
| `reference` | yes | [See field `reference`](#s-d54eb49805) |  |
| <a id="s-74632b2fee"></a>`version` | yes | type=["string","null"] |  |

##### <a id="s-d54eb49805"></a>field `reference`

- <a id="s-6659351d5c"></a>`type`: `"object"`
- <a id="s-ce6303c84e"></a>`additionalProperties`: `false`
- <a id="s-17cbde80f3"></a>`required`: `["kind","name","provider_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b263d944bd"></a>`kind` | yes | enum=["mounted-volume","listener-host"] |  |
| <a id="s-0450be0e7f"></a>`name` | yes | type="string" |  |
| <a id="s-699bbbc208"></a>`provider_id` | yes | type="string" |  |

#### <a id="s-e5866811c2"></a>`gogurt-cli-error/v1`

Applies to: operational · stdout (json).

<a id="s-69dae685ed"></a>

- <a id="s-8490ff4788"></a>`type`: `"object"`
- <a id="s-a7076148f5"></a>`additionalProperties`: `false`
- <a id="s-89cba323f4"></a>`required`: `["error"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | [See field `error`](#s-2b12ede72c) |  |

##### <a id="s-2b12ede72c"></a>field `error`

- <a id="s-0583214257"></a>`type`: `"object"`
- <a id="s-ca6d983d17"></a>`additionalProperties`: `false`
- <a id="s-71c5c012e9"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b154b06cb4"></a>`code` | yes | enum=["config_error","listener_error"] |  |
| <a id="s-30ec86eb95"></a>`message` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-6562a89368) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter name](#s-43595aca53) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Governing policies

- <a id="pa-90274e4319"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-41a26bf367"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/show/allow_extra_args`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/show/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/show/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/show/name`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/show/parameters`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/show/result_contract`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/show/parameters`

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

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/show/result_contract`

<!-- exact-contract-value: 6dd0c5b5037f512603e2e55b8ff60d3f48c482d2fba3de307e74ab663e853a93 -->

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
  "identity": "gogurt-cli-result/provider/listener-host/show/v1",
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

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/commands/show/terminating_controls`

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
