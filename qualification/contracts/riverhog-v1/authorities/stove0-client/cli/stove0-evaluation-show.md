# stove0 evaluation show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-evaluation-show:6d8ac5037d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-0d0b5bc29e"></a>Parser name: `show`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-7289ec5e97"></a>`evaluation_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | evaluation_id |

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
| <a id="s-648fa0f838"></a>`completed` | <a id="s-373b252046"></a>`{"kind":"command-completed"}` | <a id="s-56f029557a"></a>`0` | <a id="s-b82723cf0b"></a>`human: noncontractual-presentation-of-command-result; json: HTTP get_evaluation — #/components/schemas/EvaluationView` | <a id="s-81c1d94844"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-60a839b74e"></a>`usage` | <a id="s-6e3ff35566"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-3085105604"></a>`2` | <a id="s-ae24a31e90"></a>`all: empty` | <a id="s-dbe35746a8"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-da0c8dfebf"></a>`operational` | <a id="s-03fd29c84f"></a>`{"kind":"application-error"}` | <a id="s-871f115efa"></a>`1` | <a id="s-243336a520"></a>`all: empty` | <a id="s-3fd36a54a8"></a>`all: stove0-cli-diagnostic/v1` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter evaluation_id](#s-7289ec5e97) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/evaluations/{evaluation_id}](../../stove0/http-operations/get-v1-evaluations-evaluation-id.md)

## Governing policies

- <a id="pa-376a06bb23"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-8ed1e272c7"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/evaluation/commands/show/name`
- `/external_contract/cli/stove0/commands/evaluation/commands/show/parameters`
- `/external_contract/cli/stove0/commands/evaluation/commands/show/result_contract`
- `/external_contract/cli/stove0/commands/evaluation/commands/show/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

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

<!-- exact-contract-value: 917d83a4a9c212f6584ce20a719c00314b450e87d3705486d198b05faba1fd3c -->

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
        "all": "stove0-cli-diagnostic/v1"
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
