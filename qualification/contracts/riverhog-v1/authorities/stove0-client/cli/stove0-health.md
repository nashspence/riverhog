# stove0 health

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-health:3435d3e45e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-dd0f6f8ecb"></a>Parser name: `health`
- <a id="s-998e5b0787"></a>Extra arguments at this parser: rejected.
- <a id="s-bc7222ecc7"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-bef62a9e71"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-0148aceb45"></a>`ready`<br>`--ready` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5619739066"></a>`help` | <a id="s-0dd4e5fabe"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-f5e1585f72"></a>`0` | <a id="s-ff3db53d51"></a>`"noncontractual-framework-help"` | <a id="s-1efc6e35fc"></a>`"empty"` |

### Result and failure contract

- <a id="s-5d8c322e3f"></a>Result identity: `stove0-cli-result/health/v1`
- <a id="s-42231855ed"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-df5db33ebc"></a>Structured output: `optional-json`
- <a id="s-6adea3f0b3"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-72134d018e"></a>`completed` | <a id="s-33063dab99"></a>`{"kind":"command-completed"}` | <a id="s-324e8e1f27"></a>`0` | <a id="s-da84358252"></a>human: `"noncontractual-presentation-of-command-result"`; json: [OpenAPI stove0.HealthResponse](../../stove0/http-schemas/schemas-healthresponse.md) | <a id="s-99cbfd66ae"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6c139261cd"></a>`usage` | <a id="s-ef518657d4"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-575c71b251"></a>`2` | <a id="s-ebf1c9f740"></a>all: `"empty"` | <a id="s-1e06c2e43c"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-0e2aeffdcf"></a>`operational` | <a id="s-adb0c0800f"></a>`{"kind":"application-error"}` | <a id="s-8632e797e5"></a>`1` | <a id="s-748d755ba1"></a>all: `"empty"` | <a id="s-398ef99981"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"is_flag"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --ready](#s-0148aceb45) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /health/live](../../stove0/http-operations/get-health-live.md)
- [GET /health/ready](../../stove0/http-operations/get-health-ready.md)
- [stove0_api_client.Stove0ApiClient.health_live](../../stove0-api-client/python/stove0-api-client-stove0apiclient-health-live.md)
- [stove0_api_client.Stove0ApiClient.health_ready](../../stove0-api-client/python/stove0-api-client-stove0apiclient-health-ready.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-208e924490"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-9ba64b1cf2"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [reference/stove0/application/client/src/stove0\_cli/main.py::&lt;module&gt;](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/stove0/application/client/src/stove0\_cli/main.py::health](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L163)

### Machine authority

- `/external_contract/cli/stove0/commands/health/allow_extra_args`
- `/external_contract/cli/stove0/commands/health/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/health/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/health/name`
- `/external_contract/cli/stove0/commands/health/parameters`
- `/external_contract/cli/stove0/commands/health/result_contract`
- `/external_contract/cli/stove0/commands/health/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/health/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/health/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/health/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/health/name`

<!-- exact-contract-value: 69b7c75b4a0260f2e018aae0172314b00e80b3899c79ba4d71e070ba74cd8db4 -->

```json
"health"
```

### `/external_contract/cli/stove0/commands/health/parameters`

<!-- exact-contract-value: bb7aa89b48a0fb797521cb9cfad5a62b55dce8cb3a19bcda2acbd80306355cb4 -->

```json
[
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "ready",
    "nargs": 1,
    "options": [
      "--ready"
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

### `/external_contract/cli/stove0/commands/health/result_contract`

<!-- exact-contract-value: 005eed7894fdd398813667271e005f830cf8c4dcabebfb34bafbc0801f0817e8 -->

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
        "all": "noncontractual-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "stove0-cli-result/health/v1",
  "profile_id": "stove0-cli-human-json/v1",
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
          "application": "stove0",
          "definition": {
            "additionalProperties": false,
            "properties": {
              "service": {
                "minLength": 1,
                "title": "Service",
                "type": "string"
              },
              "status": {
                "const": "ok",
                "title": "Status",
                "type": "string"
              }
            },
            "required": [
              "service",
              "status"
            ],
            "title": "HealthResponse",
            "type": "object"
          },
          "kind": "openapi-schema",
          "schema": "HealthResponse"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/health/terminating_controls`

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
