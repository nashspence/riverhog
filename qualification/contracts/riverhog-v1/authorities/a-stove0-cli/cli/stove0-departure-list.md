# stove0 departure list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-cli:stove0-departure-list:9b51a9675e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-7355b8b441"></a>Parser name: `list`
- <a id="s-b406ffd10b"></a>Extra arguments at this parser: rejected.
- <a id="s-0d4cfe8f90"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-d3fd973c8a"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-5f58186d54"></a>`page_size`<br>`--page-size` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `25`<br>Env: `null` |
| <a id="s-43ad7da0ee"></a>`page_token`<br>`--page-token` | optional option; 1 value | text | not recorded<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-3b630b3a69"></a>`help` | <a id="s-5782696566"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-b7cd5f2be7"></a>`0` | <a id="s-83e79bb245"></a>`"noncontractual-framework-help"` | <a id="s-7e8cd38b0b"></a>`"empty"` |

### Result and failure contract

- <a id="s-fb5f8073cf"></a>Result identity: `stove0-cli-result/departure/list/v1`
- <a id="s-66a2105f51"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-d3536928c4"></a>Structured output: `optional-json`
- <a id="s-00213b9259"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-4347b6e238"></a>`completed` | <a id="s-056bd5b5f2"></a>`{"kind":"command-completed"}` | <a id="s-ccb349797c"></a>`0` | <a id="s-b016ffa9d2"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP list_departure_effects response 200](../../stove0/http-operations/get-v1-departure-effects.md#s-324575c5cf) | <a id="s-76ed4cc120"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6ee85edce9"></a>`usage` | <a id="s-7ca9c11be6"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-2af8c7a45a"></a>`2` | <a id="s-ee818dd7cf"></a>all: `"empty"` | <a id="s-47a98def43"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-439d33beed"></a>`operational` | <a id="s-8c584708c3"></a>`{"kind":"application-error"}` | <a id="s-992f95d6a4"></a>`1` | <a id="s-92ec2c85b4"></a>all: `"empty"` | <a id="s-6f1e9a6c75"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --page-size](#s-5f58186d54) | `value · cli-value · contract_max` | maximum=100; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-5f58186d54) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-43ad7da0ee) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/departure-effects](../../stove0/http-operations/get-v1-departure-effects.md)
- [stove0_api_client.Stove0ApiClient.list_departure_effects](../../stove0-api-client/python/stove0-api-client-stove0apiclient-list-departure-effects.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-92413c3fba"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-0376de68e0"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::list\_departure\_effects](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py#L272)

### Machine authority

- `/external_contract/cli/stove0/commands/departure/commands/list/allow_extra_args`
- `/external_contract/cli/stove0/commands/departure/commands/list/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/departure/commands/list/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/departure/commands/list/name`
- `/external_contract/cli/stove0/commands/departure/commands/list/parameters`
- `/external_contract/cli/stove0/commands/departure/commands/list/result_contract`
- `/external_contract/cli/stove0/commands/departure/commands/list/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/departure/commands/list/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/departure/commands/list/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/departure/commands/list/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/departure/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/stove0/commands/departure/commands/list/parameters`

<!-- exact-contract-value: eee9305fb9a0d69fde6f2d3f99436ec10ae34d850d4a625e38090270015a6f9f -->

```json
[
  {
    "count": false,
    "default": 25,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "page_size",
    "nargs": 1,
    "options": [
      "--page-size"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "clamp": false,
      "class": "typer._click.types.IntRange",
      "max_open": false,
      "maximum": 100,
      "min_open": false,
      "minimum": 1,
      "name": "integer range"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "page_token",
    "nargs": 1,
    "options": [
      "--page-token"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  }
]
```

### `/external_contract/cli/stove0/commands/departure/commands/list/result_contract`

<!-- exact-contract-value: 8d29bab98872ddde753f290b82b7f21f9d56a625dca1f253a0f47e2ca7ce0047 -->

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
  "identity": "stove0-cli-result/departure/list/v1",
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
          "kind": "http-operation-response",
          "method": "GET",
          "operation_id": "list_departure_effects",
          "path": "/v1/departure-effects",
          "schema": {
            "$ref": "#/components/schemas/DepartureEffectPage"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/departure/commands/list/terminating_controls`

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
