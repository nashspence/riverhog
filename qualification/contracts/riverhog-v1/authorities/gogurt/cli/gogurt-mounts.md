# gogurt mounts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-mounts:2b714e2055 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-350ba3c065"></a>Parser name: `mounts`
- <a id="s-64b5bbf570"></a>Extra arguments at this parser: rejected.
- <a id="s-94c1d8892e"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-856887834b"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-df906ece38"></a>`mounted_volume_provider`<br>`--mounted-volume-provider` | optional option; 1 value | text | not recorded<br>Env: `"GOGURT_MOUNTED_VOLUME_PROVIDER"` |
| <a id="s-498ab6f222"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-97f1f03ffd"></a>`help` | <a id="s-e4145f3821"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-9f0b3e205d"></a>`0` | <a id="s-321173a819"></a>`"noncontractual-framework-help"` | <a id="s-b35cfa92c1"></a>`"empty"` |

### Result and failure contract

- <a id="s-78b46c977e"></a>Result identity: `gogurt-cli-result/mounts/v1`
- <a id="s-45929e7d85"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-9fb02ae803"></a>Structured output: `optional-json`
- <a id="s-eca49855d8"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-00a3f402c0"></a>`completed` | <a id="s-a2d1a3fccd"></a>`{"kind":"command-completed"}` | <a id="s-5890ba2bbd"></a>`0` | <a id="s-c57f9c0623"></a>human: `"noncontractual-presentation-of-command-result"`; json: [gogurt-mounted-root-list/v1](#s-3be154750e) | <a id="s-133e8dfafd"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c4c1416683"></a>`usage` | <a id="s-8709452a47"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-22a8a7f9c0"></a>`2` | <a id="s-b934d81bab"></a>all: `"empty"` | <a id="s-50d61a0459"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-39d0526896"></a>`operational` | <a id="s-edfe26e800"></a>`{"kind":"application-error"}` | <a id="s-dd58a8cb2b"></a>`1` | <a id="s-0f8b9a0013"></a>human: `"empty"`; json: [gogurt-cli-error/v1](#s-fed5e1cfbe) | <a id="s-2f5f6ba1dc"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-3be154750e"></a>`gogurt-mounted-root-list/v1`

Applies to: completed · stdout (json).

<a id="s-41341520da"></a>

- <a id="s-ce6172f1a0"></a>`type`: `"array"`
- <a id="s-67d55f54a3"></a>`items`: type="string"

#### <a id="s-fed5e1cfbe"></a>`gogurt-cli-error/v1`

Applies to: operational · stdout (json).

<a id="s-985391c6fb"></a>

- <a id="s-238485e543"></a>`type`: `"object"`
- <a id="s-1742e0db56"></a>`additionalProperties`: `false`
- <a id="s-44c952214c"></a>`required`: `["error"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | [See field `error`](#s-84c056ec80) |  |

##### <a id="s-84c056ec80"></a>field `error`

- <a id="s-1dddb0598e"></a>`type`: `"object"`
- <a id="s-175ac531b3"></a>`additionalProperties`: `false`
- <a id="s-76cb739c51"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-02d4fe5827"></a>`code` | yes | enum=["config_error","listener_error"] |  |
| <a id="s-146679926a"></a>`message` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-498ab6f222) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --mounted-volume-provider](#s-df906ece38) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Governing policies

- <a id="pa-0319a8d3ba"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-9137284ba4"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources/authorities.md#src-3b2297c37d) — [reference/gogurt/application/src/gogurt/cli.py::&lt;module&gt;](../../../../../../reference/gogurt/application/src/gogurt/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/gogurt/commands/mounts/allow_extra_args`
- `/external_contract/cli/gogurt/commands/mounts/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/mounts/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/mounts/name`
- `/external_contract/cli/gogurt/commands/mounts/parameters`
- `/external_contract/cli/gogurt/commands/mounts/result_contract`
- `/external_contract/cli/gogurt/commands/mounts/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/mounts/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/mounts/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/mounts/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/mounts/name`

<!-- exact-contract-value: 09a1a78edad0483f330a615c7189ec42febdd6dee239f4f50c1c1809a5cb52a3 -->

```json
"mounts"
```

### `/external_contract/cli/gogurt/commands/mounts/parameters`

<!-- exact-contract-value: 83c1089fc312d926e38bc2c1895b5a74fe68544af45e9bfb4d7c6279a94e8215 -->

```json
[
  {
    "count": false,
    "envvar": "GOGURT_MOUNTED_VOLUME_PROVIDER",
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "mounted_volume_provider",
    "nargs": 1,
    "options": [
      "--mounted-volume-provider"
    ],
    "required": false,
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

### `/external_contract/cli/gogurt/commands/mounts/result_contract`

<!-- exact-contract-value: b1951034f988885f92f8f043cd191dadd18ba982a2f1d5a20860ba926123f545 -->

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
  "identity": "gogurt-cli-result/mounts/v1",
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
          "identity": "gogurt-mounted-root-list/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "items": {
              "type": "string"
            },
            "type": "array"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/gogurt/commands/mounts/terminating_controls`

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
