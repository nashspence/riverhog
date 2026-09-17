# stove0 evaluation show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-evaluation-show:89f44548f8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-0d0b5bc29e"></a>Parser name: `show`
- <a id="s-dccb7f9db8"></a>Extra arguments at this parser: rejected.
- <a id="s-ec6325b28d"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-c493f74f22"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-7289ec5e97"></a>`evaluation_id`<br>`evaluation_id` | required positional; 1 value | text | not recorded<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5b7b8cb712"></a>`help` | <a id="s-ef0f76f2ae"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-3d4b4c87dc"></a>`0` | <a id="s-9a3845edb3"></a>`"noncontractual-framework-help"` | <a id="s-2ca209030d"></a>`"empty"` |

### Result and failure contract

- <a id="s-d207315d44"></a>Result identity: `stove0-cli-result/evaluation/show/v1`
- <a id="s-9816589491"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-2a528f0496"></a>Structured output: `optional-json`
- <a id="s-634496c69e"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-648fa0f838"></a>`completed` | <a id="s-373b252046"></a>`{"kind":"command-completed"}` | <a id="s-56f029557a"></a>`0` | <a id="s-b82723cf0b"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_evaluation response 200](../../stove0/http-operations/get-v1-evaluations-evaluation-id.md#s-51f1089ac7) | <a id="s-81c1d94844"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-60a839b74e"></a>`usage` | <a id="s-6e3ff35566"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-3085105604"></a>`2` | <a id="s-ae24a31e90"></a>all: `"empty"` | <a id="s-dbe35746a8"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-da0c8dfebf"></a>`operational` | <a id="s-03fd29c84f"></a>`{"kind":"application-error"}` | <a id="s-871f115efa"></a>`1` | <a id="s-243336a520"></a>all: `"empty"` | <a id="s-3fd36a54a8"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter evaluation_id](#s-7289ec5e97) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/evaluations/{evaluation_id}](../../stove0/http-operations/get-v1-evaluations-evaluation-id.md)
- [stove0_api_client.Stove0ApiClient.get_evaluation](../../stove0-api-client/python/stove0-api-client-stove0apiclient-get-evaluation.md)

## Governing policies

- <a id="pa-5eccb9cdfc"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-ceb6f1a430"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [reference/stove0/application/client/src/stove0\_cli/main.py::&lt;module&gt;](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/stove0/application/client/src/stove0\_cli/main.py::show\_evaluation](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L424)

### Machine authority

- `/external_contract/cli/stove0/commands/evaluation/commands/show/allow_extra_args`
- `/external_contract/cli/stove0/commands/evaluation/commands/show/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/evaluation/commands/show/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/evaluation/commands/show/name`
- `/external_contract/cli/stove0/commands/evaluation/commands/show/parameters`
- `/external_contract/cli/stove0/commands/evaluation/commands/show/result_contract`
- `/external_contract/cli/stove0/commands/evaluation/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/evaluation/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/evaluation/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/evaluation/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/evaluation/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/stove0/commands/evaluation/commands/show/parameters`

<!-- exact-contract-value: ca673eb21049942d705991d78449bdc62878a1a010aacb714cce5fe46aff8f88 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "evaluation_id",
    "nargs": 1,
    "options": [
      "evaluation_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  }
]
```

### `/external_contract/cli/stove0/commands/evaluation/commands/show/result_contract`

<!-- exact-contract-value: e35c2730b92036bde6cbc8f2c57316d498acd0d90671004ae65a1c9ee4b5f55a -->

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
  "identity": "stove0-cli-result/evaluation/show/v1",
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
          "operation_id": "get_evaluation",
          "path": "/v1/evaluations/{evaluation_id}",
          "schema": {
            "$ref": "#/components/schemas/EvaluationView"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/evaluation/commands/show/terminating_controls`

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
