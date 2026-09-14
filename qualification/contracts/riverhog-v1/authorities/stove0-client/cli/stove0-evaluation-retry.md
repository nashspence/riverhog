# stove0 evaluation retry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-evaluation-retry:4449eb300a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-51b6f9fa8e"></a>Parser name: `retry`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-816618807f"></a>`evaluation_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | evaluation_id |
| <a id="s-3230ba0c3f"></a>`variant_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | variant_id |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-e6ddf515c9"></a>`help` | <a id="s-a8e7e19882"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-76d96f1125"></a>`0` | <a id="s-72d4ff6fd8"></a>`"noncontractual-framework-help"` | <a id="s-dbc44180b7"></a>`"empty"` |

### Result and failure contract

- <a id="s-9abb299042"></a>Result identity: `stove0-cli-result/evaluation/retry/v1`
- <a id="s-a48e89be39"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-1f391799a5"></a>Structured output: `optional-json`
- <a id="s-3266af78f4"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-968795b8ec"></a>`completed` | <a id="s-fad555338f"></a>`{"kind":"command-completed"}` | <a id="s-b34cba7f9e"></a>`0` | <a id="s-7261933476"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP retry_evaluation_variant response 200](../../stove0/http-operations/post-v1-evaluations-evaluation-id-variants-variant-id-retry.md#s-2718e2f1ce) | <a id="s-281c980854"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0dbc05a9f1"></a>`usage` | <a id="s-d938168691"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-fb1d1b6cce"></a>`2` | <a id="s-12feacee12"></a>all: `empty` | <a id="s-2040d58b0d"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-8e17517c87"></a>`operational` | <a id="s-7537103272"></a>`{"kind":"application-error"}` | <a id="s-dac25acf40"></a>`1` | <a id="s-7daabbd8ae"></a>all: `empty` | <a id="s-5afb5f2882"></a>all: `noncontractual-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter evaluation_id](#s-816618807f) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter variant_id](#s-3230ba0c3f) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/evaluations/{evaluation_id}/variants/{variant_id}/retry](../../stove0/http-operations/post-v1-evaluations-evaluation-id-variants-variant-id-retry.md)

## Governing policies

- <a id="pa-c877f980eb"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-e70584bb91"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/evaluation/commands/retry/name`
- `/external_contract/cli/stove0/commands/evaluation/commands/retry/parameters`
- `/external_contract/cli/stove0/commands/evaluation/commands/retry/result_contract`
- `/external_contract/cli/stove0/commands/evaluation/commands/retry/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/evaluation/commands/retry/name`

<!-- exact-contract-value: edf916f660660da65a1f21d7ab77d99621262f447acf39a4202ea81551863e66 -->

```json
"retry"
```

### `/external_contract/cli/stove0/commands/evaluation/commands/retry/parameters`

<!-- exact-contract-value: 912e9e893edee28f863c99e69ce365b0f2e227c4e5ab4f68f425a74c57bcb5a4 -->

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
  },
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "variant_id",
    "nargs": 1,
    "options": [
      "variant_id"
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

### `/external_contract/cli/stove0/commands/evaluation/commands/retry/result_contract`

<!-- exact-contract-value: 7896b7062697149c94e54476d66e03c71465406e3124bb4a655e2947171bcfbd -->

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
  "identity": "stove0-cli-result/evaluation/retry/v1",
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
          "method": "POST",
          "operation_id": "retry_evaluation_variant",
          "path": "/v1/evaluations/{evaluation_id}/variants/{variant_id}/retry",
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

### `/external_contract/cli/stove0/commands/evaluation/commands/retry/terminating_controls`

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
