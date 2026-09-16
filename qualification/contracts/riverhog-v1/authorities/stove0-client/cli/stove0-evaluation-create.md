# stove0 evaluation create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-evaluation-create:6d1ebcb1fa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-0639b8f049"></a>Parser name: `create`

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-64aae09091"></a>`definition`<br>`definition` | required positional; 1 value | path | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5ac51d30ff"></a>`help` | <a id="s-6b02dbd0f3"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-5059955552"></a>`0` | <a id="s-e3dad425be"></a>`"noncontractual-framework-help"` | <a id="s-429a53c577"></a>`"empty"` |

### Result and failure contract

- <a id="s-74eacf6ac7"></a>Result identity: `stove0-cli-result/evaluation/create/v1`
- <a id="s-ff5977ae08"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-a868448080"></a>Structured output: `optional-json`
- <a id="s-7cf3e9237c"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-46af80b7b7"></a>`completed` | <a id="s-0fa5656332"></a>`{"kind":"command-completed"}` | <a id="s-d3f0e53b88"></a>`0` | <a id="s-9b426931bf"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP create_evaluation response 201](../../stove0/http-operations/post-v1-evaluations.md#s-1c5ea98d8a) | <a id="s-ccdaa139a2"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-30d4cc4c6d"></a>`usage` | <a id="s-5108b5c49b"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-c0aa9d39b5"></a>`2` | <a id="s-f451ed53d8"></a>all: `empty` | <a id="s-7e01cc7178"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-374c3d9cbe"></a>`operational` | <a id="s-5e2da0909b"></a>`{"kind":"application-error"}` | <a id="s-bc6f34f455"></a>`1` | <a id="s-0b972869d2"></a>all: `empty` | <a id="s-ff39e53b14"></a>all: `noncontractual-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter definition](#s-64aae09091) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/evaluations](../../stove0/http-operations/post-v1-evaluations.md)
- [stove0_api_client.Stove0ApiClient.create_evaluation](../../stove0-api-client/python/stove0-api-client-stove0apiclient-create-evaluation.md)

## Governing policies

- <a id="pa-9b7f1f86df"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-1b8a6b6376"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/stove0/application/client/src/stove0_cli/main.py::create_evaluation](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L418)

### Machine authority

- `/external_contract/cli/stove0/commands/evaluation/commands/create/name`
- `/external_contract/cli/stove0/commands/evaluation/commands/create/parameters`
- `/external_contract/cli/stove0/commands/evaluation/commands/create/result_contract`
- `/external_contract/cli/stove0/commands/evaluation/commands/create/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/evaluation/commands/create/name`

<!-- exact-contract-value: 5498a731a187f424a5800943afcba027f3a6cd684e38fe6e40c02bee1753152d -->

```json
"create"
```

### `/external_contract/cli/stove0/commands/evaluation/commands/create/parameters`

<!-- exact-contract-value: 4fb330ee8d838285ee3bb2f2f8c99dccc40350eb1951a541aaaddd3d0f750f22 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "definition",
    "nargs": 1,
    "options": [
      "definition"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "path"
    }
  }
]
```

### `/external_contract/cli/stove0/commands/evaluation/commands/create/result_contract`

<!-- exact-contract-value: d5821e4eaf5145731a7124c4b738dafafc4dcc9a4a26c1265025ee4699c1175c -->

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
  "identity": "stove0-cli-result/evaluation/create/v1",
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
          "operation_id": "create_evaluation",
          "path": "/v1/evaluations",
          "schema": {
            "$ref": "#/components/schemas/EvaluationView"
          },
          "status": "201"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/evaluation/commands/create/terminating_controls`

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
